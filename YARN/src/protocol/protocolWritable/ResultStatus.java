package protocol.protocolWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import rpc.io.UTF8;
import rpc.io.Writable;

public class ResultStatus implements Writable{
	String resultStatus;
	
	
	public ResultStatus() {
		super();
		// TODO Auto-generated constructor stub
	}

	public ResultStatus(String resultStatus) {
		super();
		this.resultStatus = resultStatus;
	}

	public String getResultStatus() {
		return resultStatus;
	}

	public void setResultStatus(String resultStatus) {
		this.resultStatus = resultStatus;
	}

	@Override
	public String toString() {
		return "ResultStatus [resultStatus=" + resultStatus + "]";
	}

	@Override
	public void write(DataOutput out) throws IOException {
		UTF8.writeString(out, resultStatus);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		resultStatus=UTF8.readString(in);
		
	}

}
