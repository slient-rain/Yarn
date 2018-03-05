package rpc.net;



import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;

import com.sun.net.ssl.internal.www.protocol.https.Handler;


public class NetUtils {
	Socket socket;
	InputStream in;
	OutputStream out;
	InetSocketAddress server;
	boolean tcpNoDelay=false;
	int timeout=20000;
	/**
	 *  The max number of retries is 45,
	 * which amounts to 20s*45 = 15 minutes retries.
	 */
	int maxConnectFailureTry=45;
	int maxIOFailureTry=20;
	public void setupConnection(InetSocketAddress server) throws IOException{
		this.server=server;
		short ioFailures = 0;
		short timeoutFailures = 0;
		while (true) {
			try {        	
				socket = new Socket( );
				socket.setTcpNoDelay(tcpNoDelay);
				socket.connect(server, timeout);
				in = socket.getInputStream();
				out = socket.getOutputStream();
				System.out.println(("Connection ok"));
				break;
			} catch (SocketTimeoutException toe) {
				handleConnectionFailure(timeoutFailures++, maxConnectFailureTry, toe);
			}catch(IOException ie) {
				handleConnectionFailure(ioFailures++, maxIOFailureTry, ie);
			}
		}
	}



	private void handleConnectionFailure(
			int curRetries, int maxRetries, IOException ioe) throws IOException {
		closeConnection();
		// throw the exception if the maximum number of retries is reached
		if (curRetries >= maxRetries) {
			throw ioe;
		}
		// otherwise back off and retry
		try {
			Thread.sleep(1000);
		} catch (InterruptedException ignored) {}

		System.out.println("Retrying connect to server: " + server + 
				". Already tried " + curRetries + " time(s).");
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
	public Socket getSocket(){
		return socket;
	}
	public DataInputStream getInputStream(){
		return new DataInputStream(in);
	}
	public DataOutputStream getOutputStream(){
		return new DataOutputStream(out);
	}
}
