package rpc.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;



public class ObjectWritable implements Writable {
	private Class declaredClass;
	private Object instance;
	private static final Map<String, Class<?>> PRIMITIVE_NAMES = new HashMap<String, Class<?>>();
	static {
		PRIMITIVE_NAMES.put("boolean", Boolean.TYPE);
		PRIMITIVE_NAMES.put("byte", Byte.TYPE);
		PRIMITIVE_NAMES.put("char", Character.TYPE);
		PRIMITIVE_NAMES.put("short", Short.TYPE);
		PRIMITIVE_NAMES.put("int", Integer.TYPE);
		PRIMITIVE_NAMES.put("long", Long.TYPE);
		PRIMITIVE_NAMES.put("float", Float.TYPE);
		PRIMITIVE_NAMES.put("double", Double.TYPE);
		PRIMITIVE_NAMES.put("void", Void.TYPE);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub

	}


	/** 
	 * Write a {@link Writable}, {@link String}, primitive type, or an array of
	 * the preceding.  
	 * 
	 * @param allowCompactArrays - set true for RPC and internal or intra-cluster
	 * usages.  Set false for inter-cluster, File, and other persisted output 
	 * usages, to preserve the ability to interchange files with other clusters 
	 * that may not be running the same version of software.  Sometime in ~2013 
	 * we can consider removing this parameter and always using the compact format.
	 */
	public static void writeObject(DataOutput out, Object instance, Class declaredClass) 
			throws IOException {    
		UTF8.writeString(out, declaredClass.getName()); // always write declared

		if (declaredClass.isArray()) {     // non-primitive or non-compact array
			int length = Array.getLength(instance);
			out.writeInt(length);
			for (int i = 0; i < length; i++) {
				writeObject(out, Array.get(instance, i), declaredClass.getComponentType());
			}      
		}else if (declaredClass == String.class) {   // String
			UTF8.writeString(out, (String)instance);

		} else if (declaredClass.isPrimitive()) {     // primitive type
			if (declaredClass == Boolean.TYPE) {        // boolean
				out.writeBoolean(((Boolean)instance).booleanValue());
			} else if (declaredClass == Character.TYPE) { // char
				out.writeChar(((Character)instance).charValue());
			} else if (declaredClass == Byte.TYPE) {    // byte
				out.writeByte(((Byte)instance).byteValue());
			} else if (declaredClass == Short.TYPE) {   // short
				out.writeShort(((Short)instance).shortValue());
			} else if (declaredClass == Integer.TYPE) { // int
				out.writeInt(((Integer)instance).intValue());
			} else if (declaredClass == Long.TYPE) {    // long
				out.writeLong(((Long)instance).longValue());
			} else if (declaredClass == Float.TYPE) {   // float
				out.writeFloat(((Float)instance).floatValue());
			} else if (declaredClass == Double.TYPE) {  // double
				out.writeDouble(((Double)instance).doubleValue());
			} else if (declaredClass == Void.TYPE) {    // void
			} else {
				throw new IllegalArgumentException("Not a primitive: "+declaredClass);
			}
		} else if (declaredClass.isEnum()) {         // enum
			UTF8.writeString(out, ((Enum)instance).name());
		} else if (Writable.class.isAssignableFrom(declaredClass)) { // Writable
			UTF8.writeString(out, instance.getClass().getName());
			((Writable)instance).write(out);

		} 
		else {
			throw new IOException("Can't write: "+instance+" as "+declaredClass);
		}
	}


	/** Read a {@link Writable}, {@link String}, primitive type, or an array of
	 * the preceding. */
//	@SuppressWarnings("unchecked")
//	public static Object readObject(DataInput in, ObjectWritable objectWritable)
//			throws IOException {
//		String className = UTF8.readString(in);
//		Class<?> declaredClass = PRIMITIVE_NAMES.get(className);
//		if (declaredClass == null) {
//			declaredClass = loadClass(className);
//		}
//		Object instance=null;
//		if (declaredClass.isPrimitive()) {            // primitive types
//			if (declaredClass == Boolean.TYPE) {             // boolean
//				instance = Boolean.valueOf(in.readBoolean());
//			} else if (declaredClass == Character.TYPE) {    // char
//				instance = Character.valueOf(in.readChar());
//			} else if (declaredClass == Byte.TYPE) {         // byte
//				instance = Byte.valueOf(in.readByte());
//			} else if (declaredClass == Short.TYPE) {        // short
//				instance = Short.valueOf(in.readShort());
//			} else if (declaredClass == Integer.TYPE) {      // int
//				instance = Integer.valueOf(in.readInt());
//			} else if (declaredClass == Long.TYPE) {         // long
//				instance = Long.valueOf(in.readLong());
//			} else if (declaredClass == Float.TYPE) {        // float
//				instance = Float.valueOf(in.readFloat());
//			} else if (declaredClass == Double.TYPE) {       // double
//				instance = Double.valueOf(in.readDouble());
//			} else if (declaredClass == Void.TYPE) {         // void
//				instance = null;
//			} else {
//				throw new IllegalArgumentException("Not a primitive: "+declaredClass);
//			}
//
//		} else if (declaredClass.isArray()) {              // array
//			int length = in.readInt();
//			instance = Array.newInstance(declaredClass.getComponentType(), length);
//			for (int i = 0; i < length; i++) {
//				Array.set(instance, i, readObject(in,null));
//			}
//
//		} else if (declaredClass == String.class) {        // String
//			instance = UTF8.readString(in);
//		} else if (declaredClass.isEnum()) {         // enum
//			instance = Enum.valueOf((Class<? extends Enum>) declaredClass, UTF8.readString(in));
//		}  
//
//		if (objectWritable != null) {                 // store values
//			objectWritable.declaredClass = declaredClass;
//			objectWritable.instance = instance;
//		}
//		return instance;
//
//	}

	/** Read a {@link Writable}, {@link String}, primitive type, or an array of
	 * the preceding. */
	@SuppressWarnings("unchecked")
	public static Object readObject(DataInput in, ObjectWritable objectWritable)
			throws IOException {
		String className = UTF8.readString(in);
		Class<?> declaredClass = PRIMITIVE_NAMES.get(className);
		if (declaredClass == null) {
			declaredClass = loadClass(className);
		}

		Object instance=null;

		if (declaredClass.isPrimitive()) {            // primitive types

			if (declaredClass == Boolean.TYPE) {             // boolean
				instance = Boolean.valueOf(in.readBoolean());
			} else if (declaredClass == Character.TYPE) {    // char
				instance = Character.valueOf(in.readChar());
			} else if (declaredClass == Byte.TYPE) {         // byte
				instance = Byte.valueOf(in.readByte());
			} else if (declaredClass == Short.TYPE) {        // short
				instance = Short.valueOf(in.readShort());
			} else if (declaredClass == Integer.TYPE) {      // int
				instance = Integer.valueOf(in.readInt());
			} else if (declaredClass == Long.TYPE) {         // long
				instance = Long.valueOf(in.readLong());
			} else if (declaredClass == Float.TYPE) {        // float
				instance = Float.valueOf(in.readFloat());
			} else if (declaredClass == Double.TYPE) {       // double
				instance = Double.valueOf(in.readDouble());
			} else if (declaredClass == Void.TYPE) {         // void
				instance = null;
			} else {
				throw new IllegalArgumentException("Not a primitive: "+declaredClass);
			}

		} else if (declaredClass.isArray()) {              // array
			int length = in.readInt();
			instance = Array.newInstance(declaredClass.getComponentType(), length);
			for (int i = 0; i < length; i++) {
				Array.set(instance, i, readObject(in, objectWritable));
			}

		} 
		//	    else if (declaredClass == ArrayPrimitiveWritable.Internal.class) {
		//	      // Read and unwrap ArrayPrimitiveWritable$Internal array.
		//	      // Always allow the read, even if write is disabled by allowCompactArrays.
		//	      ArrayPrimitiveWritable.Internal temp = 
		//	          new ArrayPrimitiveWritable.Internal();
		//	      temp.readFields(in);
		//	      instance = temp.get();
		//	      declaredClass = instance.getClass();
		//
		//	    } 
		else if (declaredClass == String.class) {        // String
			instance = UTF8.readString(in);
		} else if (declaredClass.isEnum()) {         // enum
			instance = Enum.valueOf((Class<? extends Enum>) declaredClass, UTF8.readString(in));
		} 
//		else if (Message.class.isAssignableFrom(declaredClass)) {
//			instance = tryInstantiateProtobuf(declaredClass, in);
//		} 
		else {                                      // Writable
			Class instanceClass = null;
			String str = UTF8.readString(in);
			instanceClass = loadClass(str);
			Writable writable=null;
			try {
				writable = (Writable)instanceClass.newInstance();
				writable.readFields(in);
			instance = writable;
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			

			if (instanceClass == NullInstance.class) {  // null
				declaredClass = ((NullInstance)instance).declaredClass;
				instance = null;
			}
		}

		if (objectWritable != null) {                 // store values
			objectWritable.declaredClass = declaredClass;
			objectWritable.instance = instance;
		}

		return instance;

	}
	/** Return the class this is meant to be. */
	public Class getDeclaredClass() { return declaredClass; }


	/**
	 * Find and load the class with given name <tt>className</tt> by first finding
	 * it in the specified <tt>conf</tt>. If the specified <tt>conf</tt> is null,
	 * try load it directly.
	 */
	public static Class<?> loadClass( String className) {
		Class<?> declaredClass = null;
		try {
			declaredClass = Class.forName(className);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("readObject can't find class " + className,
					e);
		}
		return declaredClass;
	}
	
	
	 /**
//	   * Try to instantiate a protocol buffer of the given message class
//	   * from the given input stream.
//	   * 
//	   * @param protoClass the class of the generated protocol buffer
//	   * @param dataIn the input stream to read from
//	   * @return the instantiated Message instance
//	   * @throws IOException if an IO problem occurs
//	   */
//	  private static Message tryInstantiateProtobuf(
//	      Class<?> protoClass,
//	      DataInput dataIn) throws IOException {
//
//	    try {
//	      if (dataIn instanceof InputStream) {
//	        // We can use the built-in parseDelimitedFrom and not have to re-copy
//	        // the data
//	        Method parseMethod = getStaticProtobufMethod(protoClass,
//	            "parseDelimitedFrom", InputStream.class);
//	        return (Message)parseMethod.invoke(null, (InputStream)dataIn);
//	      } else {
//	        // Have to read it into a buffer first, since protobuf doesn't deal
//	        // with the DataInput interface directly.
//	        
//	        // Read the size delimiter that writeDelimitedTo writes
//	        int size = ProtoUtil.readRawVarint32(dataIn);
//	        if (size < 0) {
//	          throw new IOException("Invalid size: " + size);
//	        }
//	      
//	        byte[] data = new byte[size];
//	        dataIn.readFully(data);
//	        Method parseMethod = getStaticProtobufMethod(protoClass,
//	            "parseFrom", byte[].class);
//	        return (Message)parseMethod.invoke(null, data);
//	      }
//	    } catch (InvocationTargetException e) {
//	      
//	      if (e.getCause() instanceof IOException) {
//	        throw (IOException)e.getCause();
//	      } else {
//	        throw new IOException(e.getCause());
//	      }
//	    } catch (IllegalAccessException iae) {
//	      throw new AssertionError("Could not access parse method in " +
//	          protoClass);
//	    }
//	  }
//
//	  static Method getStaticProtobufMethod(Class<?> declaredClass, String method,
//	      Class<?> ... args) {
//
//	    try {
//	      return declaredClass.getMethod(method, args);
//	    } catch (Exception e) {
//	      // This is a bug in Hadoop - protobufs should all have this static method
//	      throw new AssertionError("Protocol buffer class " + declaredClass +
//	          " does not have an accessible parseFrom(InputStream) method!");
//	    }
//	  }
	
	
	private static class NullInstance  implements Writable {
	    private Class<?> declaredClass;
	    public NullInstance() { super(); }
	    public NullInstance(Class declaredClass) {
	      this.declaredClass = declaredClass;
	    }
	    @Override
	    public void readFields(DataInput in) throws IOException {
	      String className = UTF8.readString(in);
	      declaredClass = PRIMITIVE_NAMES.get(className);
	      if (declaredClass == null) {
	        try {
	          declaredClass = Class.forName(className);
	        } catch (ClassNotFoundException e) {
	          throw new RuntimeException(e.toString());
	        }
	      }
	    }
	    @Override
	    public void write(DataOutput out) throws IOException {
	      UTF8.writeString(out, declaredClass.getName());
	    }
	  }
}
