package rpc.core;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.Arrays;



import rpc.io.ObjectWritable;
import rpc.io.UTF8;
import rpc.io.Writable;

public class RPC {
	public static VersionedProtocol getProxy(Class<? extends VersionedProtocol> protocol, InetSocketAddress addr,
			int rpcTimeout){
		VersionedProtocol protocolImpl=null;
		protocolImpl=(VersionedProtocol)Proxy. newProxyInstance(
				protocol.getClassLoader(),
				new Class[]{protocol},
				new Invoker(protocol,addr,rpcTimeout));
		return protocolImpl;
	}
	
	public static Server getServer(VersionedProtocol instance,String bindAddress, int port){
		return new Server(instance,bindAddress,port);
	}
	
	 /**
	   * Stop the proxy. Proxy must either implement {@link Closeable} or must have
	   * associated {@link RpcInvocationHandler}.
	   * 
	   * @param proxy
	   *          the RPC proxy object to be stopped
	   * @throws HadoopIllegalArgumentException
	   *           if the proxy does not implement {@link Closeable} interface or
	   *           does not have closeable {@link InvocationHandler}
	   */
	  public static void stopProxy (Object proxy) {
	    if (proxy == null) {
	      System.err.println("Cannot close rpc client proxy since it is null");
	    }
	    try {
	      if (proxy instanceof Closeable) {
	        ((Closeable) proxy).close();
	        return;
	      } else {
	        InvocationHandler handler = Proxy.getInvocationHandler(proxy);
	        if (handler instanceof Closeable) {
	          ((Closeable) handler).close();
	          return;
	        }
	      }
	    } catch (IOException e) {
	      System.err.println("Closing proxy or invocation handler caused exception"+e);
	    } catch (IllegalArgumentException e) {
	    	System.err.println("RPC.stopProxy called on non proxy."+ e);
	    }
	    
	    // If you see this error on a mock object in a unit test you're
	    // developing, make sure to use MockitoUtil.mockProtocol() to
	    // create your mock.
	    System.err.println(
	        "Cannot close rpc client proxy - is not Closeable or "
	            + "does not provide closeable invocation handler "
	            + proxy.getClass());
	  }


	/**
	 * 客户端通信代理组件
	 * @author 无言的雨
	 *
	 */
	private static class Invoker implements InvocationHandler { 
		private Client.ConnectionId remoteId;
		private Client client;

		public Invoker(Class<? extends VersionedProtocol> protocol, InetSocketAddress addr,
				int rpcTimeout) {
			super();
			this.remoteId=new Client.ConnectionId(protocol, addr, rpcTimeout);
			client=new Client();
		}

		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable { 
			client.setValueClass(method.getReturnType());//设置返回值的类型
			Writable value=client.call(remoteId,new Invocation(method,args));
			return value;
		}
	}

	public static class Invocation implements Serializable,Writable {
		private static final long serialVersionUID = 1L;
		private String methodName;
		private Class[] parameterClasses;
		private Object[] parameters;
		public Invocation() {
			super();
		}
		public Invocation(Method method, Object[] parameters) {
			super();
			this.methodName = method.getName();
			this.parameterClasses = method.getParameterTypes();
			this.parameters = parameters;
		}
		public String getMethodName() {
			return methodName;
		}
		public void setMethodName(String methodName) {
			this.methodName = methodName;
		}
		public Class[] getParameterClasses() {
			return parameterClasses;
		}
		public void setParameterClasses(Class[] parameterClasses) {
			this.parameterClasses = parameterClasses;
		}
		public Object[] getParameters() {
			return parameters;
		}
		public void setParameters(Object[] parameters) {
			this.parameters = parameters;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			UTF8.writeString(out, methodName);
			out.writeInt(parameterClasses.length);
			for (int i = 0; i < parameterClasses.length; i++) {
				ObjectWritable.writeObject(out, parameters[i], parameterClasses[i]);
			}
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			methodName = UTF8.readString(in);
			parameters = new Object[in.readInt()];
			parameterClasses = new Class[parameters.length];
			ObjectWritable objectWritable = new ObjectWritable();
			if(methodName.equals("submitApplication")){
				int j=1;
			}
			for (int i = 0; i < parameters.length; i++) {
				parameters[i] = ObjectWritable.readObject(in, objectWritable);
				parameterClasses[i] = objectWritable.getDeclaredClass();
			}
		}
		
//		@Override
//		public void write(DataOutput out) throws IOException {
//			UTF8.writeString(out, methodName);
//			out.writeInt(parameterClasses.length);
//			for (int i = 0; i < parameterClasses.length; i++) {
////				out.write(parameters[i].toByteArray())
//				ObjectWritable.writeObject(out, parameterClasses[i], Message.class);
//				Message a = (Message) parameters[i];
//				out.write(a.toByteArray()); 
////				ObjectWritable.writeObject(out, parameters[i], parameterClasses[i]);
//			}
//		}
//		@Override
//		public void readFields(DataInput in) throws IOException {
//			methodName = UTF8.readString(in);
//			parameters = new Object[in.readInt()];
//			parameterClasses = new Class[parameters.length];
//			ObjectWritable objectWritable = new ObjectWritable();
//			for (int i = 0; i < parameters.length; i++) {
////				Message m=ObjectWritable.readObject(in, Message.class);
//				parameters[i] = ( ).getParserForType().parseFrom((InputStream)in);
////						ObjectWritable.readObject(in, objectWritable);
//				parameterClasses[i] = objectWritable.getDeclaredClass();
//			}
//		}
//		
//		Message createMessage(String typeName)
//		{
//		  Message message = null;
//		  final  Descriptor descriptor = DescriptorPool.generated_pool().FindMessageTypeByName(typeName);
//		  if (descriptor!=null)
//		  {
//		    final Message prototype = MessageFactory.generated_factory().GetPrototype(descriptor);
//		    if (prototype)
//		    {
//		      message = prototype->New();
//		    }
//		  }
//		  return message;
//		}

	}
}
