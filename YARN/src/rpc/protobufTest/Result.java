package rpc.protobufTest;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import rpc.io.UTF8;
import rpc.io.Writable;


import com.sun.org.apache.xml.internal.resolver.helpers.PublicId;



public class Result implements Writable {
	String result;
	public Result(){
		
	}
	public Result(String result){
		this.result=result;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		UTF8.writeString(out, result);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		result=UTF8.readString(in);

	}
	@Override
	public String toString() {
		return "Result [result=" + result + "]";
	}

}
