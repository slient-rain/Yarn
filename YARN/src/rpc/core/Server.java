package rpc.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import javax.security.sasl.SaslServer;

import rpc.io.ObjectWritable;
import rpc.io.Status;
import rpc.io.UTF8;
import rpc.io.Writable;

public class Server {
	//服务实例
	VersionedProtocol instance;
	//绑定的地址
	private String bindAddress;
	//监听的端口号
	private int port;                               // port we listen on
	//handler处理现场的个数
	private int handlerCount;                       // number of handler threads
	//read读线程的数量
	private int readThreads;                        // number of read threads
	volatile private boolean running = true;         // true while server runs
	private final boolean tcpNoDelay=true; // if T then disable Nagle's Algorithm
	//阻塞式Call待处理的队列
	private BlockingQueue<Call> callQueue; // queued calls
	private int maxQueueSize;
	//与客户端的连接数链表
	private List<Connection> connectionList = 
			Collections.synchronizedList(new LinkedList<Connection>());
	private int numConnections = 0;
	//服务端的监听线程
	private Listener listener = null;
	//处理应答线程
	private Responder responder = null;
	//处理请求线程组
	private Handler[] handlers = null;
	/** 
	 * This is set to Call object before Handler invokes an RPC and reset
	 * after the call returns.
	 */
	private static final ThreadLocal<Call> CurCall = new ThreadLocal<Call>();

	public Server(VersionedProtocol instance,String bindAddress, int port) {
		this.instance=instance;
		this.bindAddress=bindAddress;
		this.port=port;
		this.readThreads=1;
		this.handlerCount=1;
		this.maxQueueSize=10;
		this.callQueue=new LinkedBlockingQueue<Server.Call>(maxQueueSize) ;

	}

	public void start() throws IOException{
		// Start the listener here and let it bind to the port
		listener = new Listener();
		// Create the responder here
		responder = new Responder();

		//开启3大进程监听线程，回复线程，处理请求线程组
		handlers = new Handler[handlerCount];
		responder.start();
		listener.start();
		for (int i = 0; i < handlerCount; i++) {
			handlers[i] = new Handler(i);
			handlers[i].start();
		}

	}
	public Writable call(Writable param)
			throws IOException, InterruptedException, SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		Writable value=null;
		RPC.Invocation invocation = (RPC.Invocation)param;
		Method method=instance.getClass().getMethod(invocation.getMethodName(), invocation.getParameterClasses());
		value=(Writable)method.invoke(instance, invocation.getParameters());
		return value;
	}

	/**
	 * Setup response for the IPC Call.
	 * 
	 * @param response buffer to serialize the response into
	 * @param call {@link Call} to which we are setting up the response
	 * @param status {@link Status} of the IPC call
	 * @param rv return value for the IPC Call, if the call was successful
	 * @param errorClass error class, if the the call failed
	 * @param error error message, if the call failed
	 * @throws IOException
	 */
	private void setupResponse(ByteArrayOutputStream response, 
			Call call, Status status, 
			Writable rv, String errorClass, String error) 
					throws IOException {
		//写入回复的初始值，，Call的Id,Call的状态值
		response.reset();
		DataOutputStream out = new DataOutputStream(response);
		out.writeInt(call.id);                // write call id
		out.writeInt(status.state);           // write status

		if (status == Status.SUCCESS) {
			rv.write(out);
		} else {
			UTF8.writeString(out, errorClass);
			UTF8.writeString(out, error);
		}
		call.setResponse(ByteBuffer.wrap(response.toByteArray()));
	}

	/** Listens on the socket. Creates jobs for the handler threads*/
	private class Listener extends Thread {
		private ServerSocketChannel acceptChannel = null; //the accept channel
		private Selector selector = null; //the selector that we use for the server
		private Reader[] readers = null;
		private int currentReader = 0;
		private InetSocketAddress address; //the address we bind at
		private ExecutorService readPool; 

		/**
		 * 创建监听器，并通过线程池启动多个Reader线程
		 * @throws IOException
		 */
		public Listener() throws IOException {
			address = new InetSocketAddress(bindAddress, port);
			// Create a new server socket and set to non blocking mode
			acceptChannel = ServerSocketChannel.open();
			acceptChannel.configureBlocking(false);
			// Bind the server socket to the local host and port
			ServerSocket serverSocket = acceptChannel.socket();  
			serverSocket.bind(new InetSocketAddress(port));  		      
			// create a selector;
			selector= Selector.open();
			// Register accepts on the server socket with the selector.
			//Java NIO的知识，在selector上注册key的监听事件
			acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
			this.setName("IPC Server listener on " + port);
			this.setDaemon(true);

			readers = new Reader[readThreads];
			readPool = Executors.newFixedThreadPool(readThreads);
			for (int i = 0; i < readThreads; i++) {
				Selector readSelector = Selector.open();
				Reader reader = new Reader(readSelector);
				readers[i] = reader;
				//reader Runnable放入线程池中执行
				readPool.execute(reader);
			}
		}
		/**
		 * 充当mainReactor角色
		 * 服务器端轮询监听accept事件，select方法会一直阻塞直到有相关事件发生或超时  
		 */  
		@Override
		public void run() {
			while (running) {
				SelectionKey key = null;
				try {
					selector.select();
					Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
					while (iter.hasNext()) {
						key = iter.next();
						iter.remove();
						try {
							if (key.isValid()) {
								if (key.isAcceptable())
									//Listener的作用就是监听客户端的额连接事件
									doAccept(key);
							}
						} catch (IOException e) {
						}
						key = null;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				//				cleanupConnections(false);
			}
			synchronized (this) {
				try {
					acceptChannel.close();
					selector.close();
				} catch (IOException e) { }
				selector= null;
				acceptChannel= null;
				// clean up all connections
				while (!connectionList.isEmpty()) {
					closeConnection(connectionList.remove(0));
				}
			}
		}

		void doAccept(SelectionKey key) throws IOException,  OutOfMemoryError {
			Connection c = null;
			ServerSocketChannel server = (ServerSocketChannel) key.channel();
			SocketChannel channel;
			while ((channel = server.accept()) != null) {
				channel.configureBlocking(false);
				//channel.socket().setTcpNoDelay(tcpNoDelay);//???????????????????????????????????????
				Reader reader = getReader();
				try {
					//连接成功之后，在reader 监听器上注册Read读事件
					reader.startAdd();
					SelectionKey readKey = reader.registerChannel(channel);
					c = new Connection(readKey, channel, System.currentTimeMillis());
					readKey.attach(c);
					synchronized (connectionList) {
						connectionList.add(numConnections, c);
						numConnections++;
					}
				} finally {
					reader.finishAdd(); 
				}

			}
		}

		/**
		 * 轮询的方式在Reader上注册Read事件监听
		 * @return
		 */
		Reader getReader() {
			currentReader = (currentReader + 1) % readers.length;
			return readers[currentReader];
		}

		synchronized void doStop() {
			if (selector != null) {
				selector.wakeup();
				Thread.yield();
			}
			if (acceptChannel != null) {
				try {
					acceptChannel.socket().close();
				} catch (IOException e) {
					System.out.println(getName() + ":Exception in closing listener socket. " + e);
				}
			}
			readPool.shutdownNow();
		}

		private class Reader implements Runnable {
			private volatile boolean adding = false;
			private Selector readSelector = null;

			Reader(Selector readSelector) {
				this.readSelector = readSelector;
			}
			/**
			 * 充当subReactor角色
			 * 服务器端轮询监听Read事件，select方法会一直阻塞直到有相关事件发生或超时  
			 */
			@Override
			public void run() {
				synchronized (this) {
					while (running) {
						SelectionKey key = null;
						try {
							readSelector.select();
							while (adding) {
								this.wait(1000);
							}       
							Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
							while (iter.hasNext()) {
								key = iter.next();
								iter.remove();
								if (key.isValid()) {
									if (key.isReadable()) {									
										/**
										 * 当客户端channel关闭后，会不断收到read事件，但没有消息，即read方法返回-1,或执行Read方法抛出异常
										 * 所以这时服务器端也需要关闭channel，避免无限无效的处理
										 */ 
										//Reader的作用就是监听Read读事件
										doRead(key);
									}
								}
								key = null;
							}
						} catch (InterruptedException e) {
							if (running) {                          // unexpected -- log it
								System.out.println(getName() + " caught: " +
										stringifyException(e));
							}
						} catch (IOException ex) {
							ex.printStackTrace();
						}
					}
				}
			}

			void doRead(SelectionKey key) throws InterruptedException, IOException {
				int count = 0;
				Connection c = (Connection)key.attachment();
				if (c == null) {				
					key.channel().close();
					return;  
				}
				c.setLastContact(System.currentTimeMillis());			      
				try {
					//监听到RPC请求的读事件后，首先调用下面的方法
					count = c.readAndProcess();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}catch (IOException ieo) {
					/**
					 * 当客户端channel关闭后，会不断收到read事件，但没有消息，即read方法返回-1,或执行Read方法抛出IOException异常
					 * 所以这时服务器端也需要关闭channel，避免无限无效的处理,readAndProcess()会抛出IOException异常
					 */ 
					System.out.println(ieo.getMessage());
					count = -1; //so that the (count < 0) block is executed
				} 
				if (count < 0) {
					System.out.println(getName() + ": disconnecting client " + 
							c + ". Number of active connections: "+ numConnections);
					closeConnection(c);
					c = null;
				}else {
					c.setLastContact(System.currentTimeMillis());
				}
			}   


			public void startAdd() {
				adding = true;
				readSelector.wakeup();
			}

			public synchronized void finishAdd() {
				adding = false;
				this.notify();        
			}

			public synchronized SelectionKey registerChannel(SocketChannel channel)
					throws IOException {
				return channel.register(readSelector, SelectionKey.OP_READ);
			}

			public boolean isConnected(SelectionKey key){
				try{  
					((Connection)key.attachment()).channel.socket().sendUrgentData(0Xff);
					return true;
				}catch(Exception ex){  
					System.out.println("客户端已断开连接");
					return false;
				}
			}

		}
	}



	/** Handles queued calls . */
	/** 处理请求Call队列 */
	private class Handler extends Thread {
		public Handler(int instanceNumber) {
			this.setDaemon(true);
			this.setName("IPC Server handler "+ instanceNumber + " on " + port);
		}

		@Override
		public void run() {
			ByteArrayOutputStream buf = new ByteArrayOutputStream();
			//while一直循环处理
			while (running) {
				try {
					//从队列中取出call请求
					final Call call = callQueue.take(); // pop the queue; maybe blocked here
					String errorClass = null;
					String error = null;
					Writable value = null;
					//设置成当前处理的call请求
					CurCall.set(call);
					try {
						value = call( call.param);
					} catch (Throwable e) {
						errorClass = e.getClass().getName();
						error = stringifyException(e);
					}
					CurCall.set(null);
					synchronized (call.connection.responseQueue) {
						// setupResponse() needs to be sync'ed together with 
						// responder.doResponse() since setupResponse may use
						// SASL to encrypt response data and SASL enforces
						// its own message ordering.
						//设置回复初始条件
						setupResponse(buf, call, (error == null) ? Status.SUCCESS : Status.ERROR, 
								value, errorClass, error);
						// Discard the large buf and reset it back to 
						// smaller size to freeup heap

						//交给responder线程执行写回复操作
						responder.doRespond(call);
					}
				} catch (InterruptedException e) {
					if (running) {                          // unexpected -- log it
						System.out.println(getName() + " caught: " +
								stringifyException(e));
					}
				}catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}




	// Sends responses of RPC back to clients.
	/** 发送请求回复给客户端 */
	private class Responder extends Thread {
		private Selector writeSelector;
		private int pending;         // connections waiting to register

		final static int PURGE_INTERVAL = 900000; // 15mins

		Responder() throws IOException {
			this.setName("IPC Server Responder");
			this.setDaemon(true);
			writeSelector = Selector.open(); // create a selector
			pending = 0;
		}

		@Override
		public void run() {
			while (running) {
				try {
					waitPending();     // If a channel is being registered, wait.
					writeSelector.select(PURGE_INTERVAL);
					Iterator<SelectionKey> iter = writeSelector.selectedKeys().iterator();
					while (iter.hasNext()) {
						SelectionKey key = iter.next();
						iter.remove();
						try {
							if (key.isValid() && key.isWritable()) {
								//异步的写事件
								doAsyncWrite(key);
							}
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		}

		private void doAsyncWrite(SelectionKey key) throws IOException {
			Call call = (Call)key.attachment();
			if (call == null) {
				return;
			}
			synchronized(call.connection.responseQueue) {
				//调用了核心的processResponse处理回复的方法
				if (processResponse(call.connection.responseQueue)) {
					try {
						key.interestOps(0);
					} catch (Exception e) {
						/* The Listener/reader might have closed the socket.
						 * We don't explicitly cancel the key, so not sure if this will
						 * ever fire.
						 * This warning could be removed.
						 */
						e.printStackTrace();
					}
				}
			}
		}

		// Processes one response. Returns true if there are no more pending
		// data for this channel.
		private boolean processResponse(LinkedList<Call> responseQueue) throws IOException {
			boolean error = true;
			boolean done = false;       // there is more data for this channel.
			int numElements = 0;
			Call call = null;
			synchronized (responseQueue) {				
				numElements = responseQueue.size();
				if (numElements == 0) {
					return true;              // no more data for this channel.
				}			
				call = responseQueue.removeFirst();
				SocketChannel channel = call.connection.channel;
				channel.write(call.response);
				if (!call.response.hasRemaining()) {
					//call.connection.decRpcCount();
					if (numElements == 1) {    // last call fully processes.
						done = true;             // no more data for this channel.
					} else {
						done = false;            // more calls pending to be sent.
					}

				} else {
					//
					// If we were unable to write the entire response out, then 
					// insert in Selector queue. 
					//重新把这个call加回call列表
					call.connection.responseQueue.addFirst(call);

					//if (inHandler) {
					//inHandler说明此回复将会过会被发送回去，需要改写时间
					// set the serve time when the response has to be sent later
					//改写Call中收到回复的时间
					//call.timestamp = System.currentTimeMillis();

					incPending();
					try {
						// Wakeup the thread blocked on select, only then can the call 
						// to channel.register() complete.
						writeSelector.wakeup();
						channel.register(writeSelector, SelectionKey.OP_WRITE, call);
					} catch (ClosedChannelException e) {
						//Its ok. channel might be closed else where.
						done = true;
					} finally {
						decPending();
					}
					//}
					return true;
				}
			}
			return done;
		}

		//
		// 注册写监听事件，然后交由Responder进行处理
		//
		void doRespond(Call call) throws IOException {
			synchronized (call.connection.responseQueue) {
				call.connection.responseQueue.addLast(call);
				incPending();
				SocketChannel channel = call.connection.channel;		          
				writeSelector.wakeup();
				channel.register(writeSelector, SelectionKey.OP_WRITE, call);
				decPending();
			}
		}
		private synchronized void waitPending() throws InterruptedException {
			while (pending > 0) {
				wait();
			}
		}
		private synchronized void incPending() {   // call waiting to be enqueued.
			pending++;
		}
		private synchronized void decPending() { // call done enqueueing.
			pending--;
			notify();
		}
	}
	/**
	 * Make a string representation of the exception.
	 * @param e The exception to stringify
	 * @return A string with exception name and call stack.
	 */
	public String stringifyException(Throwable e) {
		StringWriter stm = new StringWriter();
		PrintWriter wrt = new PrintWriter(stm);
		e.printStackTrace(wrt);
		wrt.close();
		return stm.toString();
	}

	/** cleanup connections from connectionList. Choose a random range
	 * to scan and also have a limit on the number of the connections
	 * that will be cleanedup per run. The criteria for cleanup is the time
	 * for which the connection was idle. If 'force' is true then all 
	 * connections will be looked at for the cleanup.
	 */
	//    private void cleanupConnections(boolean force) {
	//      if (force || numConnections > thresholdIdleConnections) {
	//        long currentTime = System.currentTimeMillis();
	//        if (!force && (currentTime - lastCleanupRunTime) < cleanupInterval) {
	//          return;
	//        }
	//        int start = 0;
	//        int end = numConnections - 1;
	//        if (!force) {
	//          start = rand.nextInt() % numConnections;
	//          end = rand.nextInt() % numConnections;
	//          int temp;
	//          if (end < start) {
	//            temp = start;
	//            start = end;
	//            end = temp;
	//          }
	//        }
	//        int i = start;
	//        int numNuked = 0;
	//        while (i <= end) {
	//          Connection c;
	//          synchronized (connectionList) {
	//            try {
	//              c = connectionList.get(i);
	//            } catch (Exception e) {return;}
	//          }
	//          if (c.timedOut(currentTime)) {
	//            if (LOG.isDebugEnabled())
	//              LOG.debug(getName() + ": disconnecting client " + c.getHostAddress());
	//            closeConnection(c);
	//            numNuked++;
	//            end--;
	//            c = null;
	//            if (!force && numNuked == maxConnectionsToNuke) break;
	//          }
	//          else i++;
	//        }
	//        lastCleanupRunTime = System.currentTimeMillis();
	//      }
	//    }

	private void closeConnection(Connection connection) {
		synchronized (connectionList) {
			if (connectionList.remove(connection))
				numConnections--;
		}
		try {
			connection.close();
		} catch (IOException e) {
		}
	}

	/** Stops the service.  No new calls will be handled after this is called. */
	public synchronized void stop() {
		System.out.println("Stopping server on " + port);
		running = false;
		if (handlers != null) {
			for (int i = 0; i < handlerCount; i++) {
				if (handlers[i] != null) {
					handlers[i].interrupt();
				}
			}
		}
		listener.interrupt();
		listener.doStop();
		responder.interrupt();
		notifyAll();
	}

	/**
	 * 来自统一个客户端（同一个channel）的请求共用同一个Connection，Call成员属性connection相同
	 * 多个call处理完后结果放入responseQueue中
	 */
	/** Reads calls from a connection and queues them for handling. */
	public class Connection {
		private SocketChannel channel;
		//回复Call列表
		private LinkedList<Call> responseQueue;//
		//字节缓冲用于读写回复
		private ByteBuffer data;
		private ByteBuffer dataLengthBuffer;//用于读取一个整形数据，param数据的长度
		private int dataLength;

		//此连接下的RPC请求数
		private volatile int rpcCount = 0; // number of outstanding rpcs
		private long lastContact;
		private Socket socket;
		// Cache the remote host & port info so that even if the socket is 
		// disconnected, we can say where it used to connect to.
		private String hostAddress;
		private int remotePort;
		private InetAddress addr;


		public Connection(SelectionKey key, SocketChannel channel, 
				long lastContact) {
			this.channel = channel;
			this.lastContact = lastContact;
			this.data = null;
			this.dataLengthBuffer = ByteBuffer.allocate(4);
			this.socket = channel.socket();
			this.addr = socket.getInetAddress();
			if (addr == null) {
				this.hostAddress = "*Unknown*";
			} else {
				this.hostAddress = addr.getHostAddress();
			}
			this.remotePort = socket.getPort();
			this.responseQueue = new LinkedList<Call>();
			this.dataLengthBuffer = ByteBuffer.allocate(4);

		}  

		public int readAndProcess() throws IOException, InterruptedException  {
			int count=-1;
			//			dataLengthBuffer.clear();  
			//			count = channel.read(dataLengthBuffer);  
			if (data == null) {
				data = ByteBuffer.allocate(1000);
			}
			//			dataLengthBuffer.flip();
			//			dataLength = dataLengthBuffer.getInt();
			//继承从channel通道读入数据到data中

			//if(isConnected(channel)){
			data.clear();

			if((count = channel.read(data))==-1){
				return count;
			}
			processData(data.array());

			return count;
		}

		public boolean isConnected(SocketChannel channel){
			try{  
				channel.socket().sendUrgentData(0xFF); 
				return true;
			}catch(Exception ex){  
				System.out.println("客户端已断开连接");
				return false;
			}
		}

		private void processData(byte[] buf) throws  IOException, InterruptedException {
			DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buf));
			int id = dis.readInt();// try to read client call id		      
			//从配置根据反射获取参数类型
			Writable param = new RPC.Invocation();
			//数据读入此类似
			param.readFields(dis);    
			//依据ID，和参数构建Server服务的Call回调对象
			Call call = new Call(id, param, this);
			//放入阻塞式Call队列
			callQueue.put(call);              // queue the call; maybe blocked here
			//增加RPC请求数的数量
			incRpcCount();  // Increment the rpc count
		}

		/* Increment the outstanding RPC count */
		private void incRpcCount() {
			rpcCount++;
		}

		public void setLastContact(long lastContact) {
			this.lastContact = lastContact;
		}

		private synchronized void close() throws IOException {			
			data = null;
			dataLengthBuffer = null;
			if (!channel.isOpen())
				return;
			try {socket.shutdownOutput();} catch(Exception e) {}
			if (channel.isOpen()) {
				try {channel.close();} catch(Exception e) {}
			}
			try {socket.close();} catch(Exception e) {}
		}

	}

	/** A call queued for handling. */
	/** 服务端的Call列表队列 ，与客户端的是不同的*/
	private static class Call {
		//客户端的Call Id,是从客户端上传过类的
		private int id;                               // the client's call id
		//Call回调参数
		private Writable param;                       // the parameter passed
		//还保存了与客户端的连接
		private Connection connection;                // connection to client

		//接收到response回应的时间
		private long timestamp;     // the time received when response is null
		// the time served when response is not null
		//对于此回调的回应值
		private ByteBuffer response;                      // the response for this call

		public Call(int id, Writable param, Connection connection) { 
			this.id = id;
			this.param = param;
			this.connection = connection;
			this.timestamp = System.currentTimeMillis();
			this.response = null;
		}

		@Override
		public String toString() {
			return param.toString() + " from " + connection.toString();
		}

		public void setResponse(ByteBuffer response) {
			this.response = response;
		}
	}

}
