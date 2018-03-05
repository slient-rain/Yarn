package client.test;



import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import protocol.protocolBuf.ApplicationIdProtos.ApplicationId;
import protocol.protocolBuf.GetNewApplicationResoponseProtos.GetNewApplicationResponse;
import protocol.protocolBuf.ResourceProtos;

import client.YarnClient;
import client.YarnClientImpl;
import resourceManager.ClientRMService.ClientRMService;

public class Main {
	public static void main(String[] args) {
		YarnClient client=new YarnClientImpl();
		client.init();
		client.start();
		System.out.println(client.createApplication());
//		try {
//			
//			OutputStream os = new FileOutputStream("src//testfile.txt");
//			GetNewApplicationResponse applicationId=GetNewApplicationResponse.newBuilder().setApplicationId(ApplicationId.newBuilder().setAppIdStrPrefix("application_").setId(1).setClusterTimestamp(2015).build()).setMaximumResourceCapability(ResourceProtos.Resource.newBuilder().setMemory(1).setVCores(1)).build();
//			os.write(applicationId.toByteArray()); 
//			os.close();
//			InputStream is = new FileInputStream("src//testfile.txt");
//			GetNewApplicationResponse applicationIdNew=GetNewApplicationResponse.parseFrom(is);
//			System.out.println(applicationId.toString());
//			
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		  
	}
	
	
}
