package rpc.protobufTest;
import java.io.IOException;

import rpc.core.RPC;
import rpc.core.Server;



public class ServerMain {
	public static void main(String[] args) {
		JobSubmissionProtocolImpl instance=new JobSubmissionProtocolImpl();
		Server server=RPC.getServer(instance, "localhost", 8080);
		try {
			server.start();
			System.out.println("服务器启动成功");
//			Thread.sleep(3000);
//			server.stop();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	}
}
