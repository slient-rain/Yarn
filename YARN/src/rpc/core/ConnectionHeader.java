package rpc.core;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import rpc.io.Writable;



/**
 * The IPC connection header sent by the client to the server
 * on connection establishment.
 */
public class ConnectionHeader implements Writable{	  
	  private String protocol;	  
	  public ConnectionHeader() {}
	  
	  
	  public ConnectionHeader(String protocol) {
	    this.protocol = protocol;
	  }

	  @Override
	  public void readFields(DataInput in) throws IOException {
	    
	  }

	  @Override
	  public void write(DataOutput out) throws IOException {
	    
	  }

	  public String getProtocol() {
	    return protocol;
	  }
}