package rpc.protobufTest;


import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import protocol.protocolBuf.ResourceProtos;
import protocol.protocolBuf.ApplicationIdProtos.ApplicationId;
import protocol.protocolBuf.GetNewApplicationResoponseProtos.GetNewApplicationResponse;

import rpc.core.RPC;




public class ClientMain {
	public static void main(String[] args) {
		
		try {
			InetSocketAddress addr = new InetSocketAddress("localhost",8080);
			JobSummissionProtocol proxy=(JobSummissionProtocol)RPC.getProxy(JobSummissionProtocol.class, addr, 0);
			//while(true){
				System.out.println(proxy.getHelloWord(GetNewApplicationResponse.newBuilder().setApplicationId(ApplicationId.newBuilder().setAppIdStrPrefix("application_").setId(1).setClusterTimestamp(2015).build()).setMaximumResourceCapability(ResourceProtos.Resource.newBuilder().setMemory(1).setVCores(1)).build()).toString());	
				//Thread.sleep(10000);
				
			//}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
