package rpc.core;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import rpc.io.DataOutputBuffer;
import rpc.io.IOUtils;
import rpc.io.Status;
import rpc.io.UTF8;
import rpc.io.Writable;
import rpc.net.NetUtils;
import rpc.test.Result;



public class Client {
	private int counter=2; // counter for call ids
	/**
	 * 连接池，客户端到服务端的连接
	 */
	private ConcurrentHashMap<ConnectionId, Connection> connections =
			new ConcurrentHashMap<ConnectionId, Connection>();

	/**
	 * 回调值类           extends Writable
	 */
	private Class<? > valueClass;   // class of call values

	//原子变量判断客户端是否还在运行
	private AtomicBoolean running = new AtomicBoolean(true); // if client runs


	/** 
	 * Make a call, passing param to the IPC server defined by remoteId,
	 * returning the value.  
	 * Throws exceptions if there are network problems or if the remote code 
	 * threw an exception. 
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public Writable call(ConnectionId remoteId, Writable param) throws IOException, InterruptedException{
		//根据参数构造一个Call回调
		Call call = new Call(param);
		//根据远程ID获取连接
		Connection connection = getConnection(remoteId, call);
		//发送参数
		connection.sendParam(call);                 
		boolean interrupted = false;
		synchronized (call) {
			//等待/通知模式，直到call得到返回结果
			while (!call.done) {
				try {
					//等待远端程序的执行完毕
					call.wait();                           // wait for the result
				} catch (InterruptedException ie) {
					// save the fact that we were interrupted
					interrupted = true;
				}
			}
			//如果是异常中断，则终止当前线程
			if (interrupted) {
				// set the interrupt flag now that we are done waiting
				Thread.currentThread().interrupt();
			}
			//如果call回到出错，则返回call出错信息
			//			if (call.error != null) {
			//				if (call.error instanceof RemoteException) {
			//				call.error.fillInStackTrace();
			//					          throw call.error;
			//					        } else { // local exception
			//					          // use the connection because it will reflect an ip change, unlike
			//					          // the remoteId
			//					          throw wrapException(connection.getRemoteAddress(), call.error);
			//					        }
			//			} else {
			//如果是正常情况下，返回回调处理后的值
			return call.value;
			//			}
		}
	}

	/** 
	 * Get a connection from the pool, or create a new one and add it to the
	 * pool.  Connections to a given ConnectionId are reused. 
	 */
	private Connection getConnection(ConnectionId remoteId,Call call)
			throws IOException, InterruptedException {
		Connection connection;
		/** 
		 * we could avoid this allocation for each RPC by having a  
		 * connectionsId object and with set() method. We need to manage the
		 * refs for keys in HashMap properly. For now its ok.
		 */
		do {
			synchronized (connections) {
				//从connection连接池中获取连接，可以保证相同的连接ID可以复用
				connection = connections.get(remoteId);
				if (connection == null) {
					connection = new Connection(remoteId);
					connections.put(remoteId, connection);
				}
			}
		} while (!connection.addCall(call));

		//we don't invoke the method below inside "synchronized (connections)"
		//block above. The reason for that is if the server happens to be slow,
		//it will take longer to establish a connection and that will slow the
		//entire system down.
		connection.setupIOstreams();
		return connection;
	}

	public Class<? > getValueClass() {
		return valueClass;
	}

	public void setValueClass(Class<?> writable) {
		this.valueClass = writable;
	}

	/** 
	 * 客户端网路通信组件
	 * run()循环进行接收Response判断
	 * 事件触发sendParam()
	 */
	private class Connection extends Thread {
		//所连接的服务器地址
		private InetSocketAddress server;             // server ip:port
		//远程连接ID 
		private final ConnectionId remoteId;                // connection id

		//下面是一组socket通信方面的变量
		private Socket socket = null;                 // connected socket
		private DataInputStream in;
		private DataOutputStream out;

		// currently active calls 当前活跃的回调，一个连接 可能会有很多个call回调
		private ConcurrentHashMap<Integer, Call> calls = new ConcurrentHashMap<Integer, Call>();

		//连接关闭标记
		private AtomicBoolean shouldCloseConnection = new AtomicBoolean();  // indicate if the connection is closed
		private IOException closeException; // close reason
		private int maxIdleTime=30000; //connections will be culled if it was idle for
		//maxIdleTime msecs
		//最后一次IO活动通信的时间
		private AtomicLong lastActivity = new AtomicLong();

		public Connection(ConnectionId remoteId) throws IOException {
			this.server = remoteId.getAddress();
			this.remoteId = remoteId;	        
			if (server.isUnresolved()) {
				throw new UnknownHostException("unknown host: " + remoteId.getAddress().getHostName());
			}	        
			Class<?> protocol = remoteId.getProtocol();	        
			InetSocketAddress addr = remoteId.getAddress();
			this.setName("IPC Client ("  +") connection to " +
					remoteId.getAddress().toString() );
			/**
			 * 设置守护线程，当没有其他线程运行时，该线程会自动关闭
			 * 如果设置成非守护县城，当主线程运行结束后，该线程同样能继续运行
			 */
			this.setDaemon(true);
		}

		/**
		 * Add a call to this connection's call queue and notify
		 * a listener; synchronized.                            listener????????????????????????????
		 * Returns false if called during shutdown.
		 * @param call to add
		 * @return true if the call was added.
		 */
		private synchronized boolean addCall(Call call) {
			if (shouldCloseConnection.get())
				return false;
			calls.put(call.id, call);
			notify();                     //????????????????????????????
			return true;
		}

		/** 
		 * 创建网络通信连接
		 * Connect to the server and set up the I/O streams. It then sends
		 * a header to the server and starts
		 * the connection thread that waits for responses.
		 * @throws IOException 
		 */
		private synchronized void setupIOstreams()  {
			try{
				if (socket != null || shouldCloseConnection.get() ) {
					return;
				}			
				NetUtils utils=new NetUtils();
				utils.setupConnection(server);
				this.socket=utils.getSocket();
				this.in=utils.getInputStream();
				this.out=utils.getOutputStream();	
				// update last activity time
				touch();
				start();
				return;
			}
			//			catch (SocketTimeoutException e) {
			//				markClosed(new SocketTimeoutException("Couldn't set up IO streams", e));
			//			}
			catch (IOException e) {//？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？异常的创建和处理
				markClosed(new IOException("Couldn't set up IO streams", e));
			}
			close();
		}


		/**
		 * Client的run执行方法
		 */
		public void run() {
			//等待工作，等待请求调用
			while (waitForWork()) {//wait here for work - read or close connection			
				//调用完请求，则立即获取回复
				receiveResponse();
			}
			close();

		}	    



		/** 
		 * Initiates a call by sending the parameter to the remote server.
		 * Note: this is not called from the Connection thread, but by other
		 * threads.
		 */
		public void sendParam(Call call) {
			if (shouldCloseConnection.get()) {
				return;
			}
			DataOutputBuffer d=null;
			try {
				synchronized (this.out) {
					//for serializing the data to be written
					//将call回调中的参数写入到输出流中，传向服务端
					d = new DataOutputBuffer();
					d.writeInt(call.id);
					call.param.write(d);
					byte[] data = d.getData();
					int dataLength = d.getLength();
					//					out.writeInt(dataLength);      //first put the data length
					//					out.writeInt(dataLength);      //first put the data length
					//					out.writeInt(dataLength);      //first put the data length
					out.write(data, 0, dataLength);//write the data
					out.flush();
				}
			} catch(IOException e) {
				markClosed(e);
			} finally{
				IOUtils.closeStream(d);
			}
		}  

		/* Receive a response.
		 * Because only one receiver, so no synchronization on in.
		 * 获取回复值
		 */
		private void receiveResponse() {
			if (shouldCloseConnection.get()) {
				return;
			}
			//更新最近一次的call活动时间
			touch();
			try {
				System.out.println("enter receiveResponse");
				int id = in.readInt();                    // try to read an id
				//从获取call中取得相应的call
				Call call = calls.get(id);
				//判断该结果状态
				int state = in.readInt();     // read call status
				if (state == Status.SUCCESS.state) {
					//Method readFields=valueClass.getMethod("readFields", new Class[]{DataInput.class});
					Writable value=(Writable)valueClass.newInstance();
					value.readFields(in);                 // read value
					call.setValue(value);
					calls.remove(id);
				} else if (state == Status.ERROR.state) {
					call.setException(new IOException(UTF8.readString(in)+ UTF8.readString(in)));
					calls.remove(id);
				} else if (state == Status.FATAL.state) {
					// Close the connection
					markClosed(new IOException(UTF8.readString(in)+ UTF8.readString(in)));
				}
			} catch (IOException e) {
				e.printStackTrace();
				markClosed(e);
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}

		

		/**
		 * 判断是否关闭连接
		 * wait till someone signals us to start reading RPC response or
		 * it is idle too long, it is marked as to be closed, 
		 * or the client is marked as not running.
		 * 
		 * Return true if it is time to read a response; false otherwise.
		 */
		private synchronized boolean waitForWork() {
			/**
			 * 所有请求处理完毕之后，睡眠等待一段时间，如果还没有请求再关闭
			 */
			if (calls.isEmpty() && running.get() && !shouldCloseConnection.get()  )  {
				long timeout = maxIdleTime-
						(System.currentTimeMillis()-lastActivity.get());
				if (timeout>0) {
					try {
						wait(timeout);
					} catch (InterruptedException e) {}
				}
			}
			if (!calls.isEmpty()&& running.get()&& !shouldCloseConnection.get() ) { 
				return true;
			}else if (shouldCloseConnection.get()) {
				return false;
			}else if (calls.isEmpty()) { // idle connection closed or stopped
				markClosed(null);
				return false;
			}else { // get stopped but there are still pending requests 
				markClosed((IOException)new IOException().initCause(
						new InterruptedException()));
				return false;
			}
		}

		/** 
		 * Close the connection. 
		 * 并进行异常的记录和处理
		 */
		private synchronized void close() {
			if (!shouldCloseConnection.get()) {
				return;
			}
			// release the resources
			// first thing to do;take the connection out of the connection list
			synchronized (connections) {
				if (connections.get(remoteId) == this) {
					connections.remove(remoteId);
				}
			}
			// close the streams and therefore the socket
			IOUtils.closeStream(out);
			IOUtils.closeStream(in);
			closeConnection();
			System.out.println("关闭客户端连接");
			//disposeSasl();

			// clean up all calls
			if (closeException == null) {
				if (!calls.isEmpty()) {
					System.out.println("A connection is closed for no cause and calls are not empty");
					// clean up calls anyway
					closeException = new IOException("Unexpected closed connection");
					cleanupCalls();
				}
			} else {
				//				// log the info
				//				if (LOG.isDebugEnabled()) {
				System.out.println("closing ipc connection to " + server + ": " +
						closeException.getMessage()+"  "+closeException);
				//				}

				// cleanup calls
				cleanupCalls();
			}
			//			if (LOG.isDebugEnabled())
			System.out.println(getName() + ": closed");
		}

		public void closeConnection() {
			// close the current connection
			try {
				socket.close();
			} catch (IOException e) {
				System.out.println(("Not able to close a socket"+e.getMessage()));
			}
			// set socket to null so that the next call to setupIOstreams
			// can start the process of connect all over again.
			socket = null;
		}

		/** Update lastActivity with the current time. */
		private void touch() {
			lastActivity.set(System.currentTimeMillis());
		}

		private synchronized void markClosed(IOException e) {
			if (shouldCloseConnection.compareAndSet(false, true)) {
				closeException = e;
				notifyAll();                 //？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？
			}
		}
		/* Cleanup all calls and mark them as done */
		private void cleanupCalls() {
			Iterator<Entry<Integer, Call>> itor = calls.entrySet().iterator() ;
			while (itor.hasNext()) {
				Call c = itor.next().getValue(); 
				c.setException(closeException); // local exception
				itor.remove();         
			}
		}


	} 


	/** 
	 * 客户端的一个回调 ,对调用参数和回掉结果，以及回掉是否完成监控状态的封装
	 */
	private class Call {
		//回调ID
		int id;                                       // call id
		//被序列化的参数
		Writable param;                               // parameter
		//返回值
		Writable value;                               // value, null if error
		//出错时返回的异常
		IOException error;                            // exception, null if value
		//回调是否已经被完成
		boolean done;                                 // true when call is done

		protected Call(Writable param) {
			this.param = param;
			synchronized (Client.this) {
				//根据counter计数器递增call ID
				this.id = counter++;
			}
		}

		/** Indicate when the call is complete and the
		 * value or error are available.  Notifies by default.  */
		protected synchronized void callComplete() {
			//调用完成后，把done布尔值设置为true
			this.done = true;
			//并通知回调
			notify();                                 // notify caller
		}

		/** Set the exception when there is an error.
		 * Notify the caller the call is done.
		 * 
		 * @param error exception thrown by the call; either local or remote
		 */
		public synchronized void setException(IOException error) {
			this.error = error;
			callComplete();
		}

		/** Set the return value when there is no error. 
		 * Notify the caller the call is done.
		 * 
		 * @param value return value of the call.
		 */
		public synchronized void setValue(Writable value) {
			this.value = value;
			callComplete();
		}
	}

	/**
	 * This class holds the address and the user ticket. The client connections
	 * to servers are uniquely identified by <remoteAddress, protocol, ticket>
	 * 连接的唯一标识，主要通过<远程地址，协议类型>
	 */
	static class ConnectionId{
		Class<?> protocol; 		
		InetSocketAddress addr;
		int rpcTimeout;//暂时并未使用

		public ConnectionId(Class<?> protocol,
				InetSocketAddress addr, int rpcTimeout) {
			super();
			this.protocol = protocol;
			this.addr = addr;
			this.rpcTimeout = rpcTimeout;
		}

		public Class<?> getProtocol() {
			return protocol;
		}

		public void setProtocol(Class<?> protocol) {
			this.protocol = protocol;
		}

		public InetSocketAddress getAddress() {
			return addr;
		}

		public void setAddr(InetSocketAddress addr) {
			this.addr = addr;
		}

		public int getRpcTimeout() {
			return rpcTimeout;
		}

		public void setRpcTimeout(int rpcTimeout) {
			this.rpcTimeout = rpcTimeout;
		}
	}
}
