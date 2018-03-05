package nodeManager.nodeManagerService.test;

import java.io.IOException;

import nodeManager.nodeManagerService.NodeManagerService;

import protocol.protocol.ContainerManagementProtocol;

import rpc.core.RPC;
import rpc.core.Server;
import rpc.test.JobSubmissionProtocolImpl;

public class Main {
	public static void main(String[] args) {
		ContainerManagementProtocol instance=new NodeManagerService();
		Server server=RPC.getServer(instance, "localhost", 82);
		try {
			server.start();
			System.out.println("服务器启动成功");
			//		Thread.sleep(3000);
			//		server.stop();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
}
