package rpc.test;


import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import protocol.protocol.ApplicationClientProtocol;
import protocol.protocolWritable.ApplicationSubmissionContext;

import rpc.core.RPC;




public class ClientMain {
	public static void main(String[] args) {
		try {
			InetSocketAddress addr = new InetSocketAddress("localhost",8080);
			ApplicationClientProtocol proxy=(ApplicationClientProtocol)RPC.getProxy(ApplicationClientProtocol.class, addr, 0);
			//while(true){
				System.out.println(proxy.getNewApplication().toString());
//			proxy.submitApplication(new ApplicationSubmissionContext());
				//Thread.sleep(10000);
				
			//}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
//		try {
//			InetSocketAddress addr = new InetSocketAddress("localhost",8080);
//			JobSummissionProtocol proxy=(JobSummissionProtocol)RPC.getProxy(JobSummissionProtocol.class, addr, 0);
//			//while(true){
//				System.out.println(proxy.getHelloWord(false).toString());	
//				//Thread.sleep(10000);
//				
//			//}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		
	}
}
