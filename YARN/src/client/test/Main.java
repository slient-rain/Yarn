package client.test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import protocol.protocolBuf.ApplicationIdProtos.ApplicationId;
import protocol.protocolBuf.GetNewApplicationResoponseProtos.GetNewApplicationResponse;
import protocol.protocolBuf.ResourceProtos;
import protocol.protocolWritable.ApplicationSubmissionContext;
import protocol.protocolWritable.ApplicationSubmissionContext.LocalResource;
import protocol.protocolWritable.ApplicationSubmissionContext.URL;
import client.Client;
import client.YarnClient;
import client.YarnClientApplication;
import client.YarnClientImpl;
import resourceManager.ClientRMService.ClientRMService;
import resourceManager.scheduler.Priority;
import resourceManager.scheduler.Resource;
import resourceManager.scheduler.Scheduler;

public class Main {
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);
	public static void main(String[] args) {
		Client client = new Client();
		client.yarnClient.init();
		client.yarnClient.start();
		YarnClientApplication application=client.yarnClient.createApplication();
		ApplicationSubmissionContext submissionContext=application.getApplicationSubmissionContext();
		submissionContext.setApplicationName("测试应用");
		submissionContext.setCommand(Arrays.asList("java -jar","task0.jar"));
		String applicationId=application.getNewApplicationResponse().getApplicationId().toString();
		Map<String,ApplicationSubmissionContext.LocalResource> map=new HashMap<String, ApplicationSubmissionContext.LocalResource>();
		for(int i=0; i<1; i++){
			String file="task"+i+".jar";
			URL resource=submissionContext.new URL("192.168.2.134",8080,"AppStore/Download?applicationId="+applicationId+"&file="+file);
			LocalResource localResouce =submissionContext.new LocalResource();
			localResouce.setResource(resource);
			localResouce.setSize(500);
			localResouce.setTimestamp(20170530);
			localResouce.setType("file");
			map.put("file"+i, localResouce);
		}
		submissionContext.setLocalResouces(map);	
		submissionContext.setPriority(new Priority(1));
		submissionContext.setQueue("queue 1");
		submissionContext.setResource(new Resource(1, 1));
		submissionContext.setUser("root");
		client.yarnClient.submitApplication(submissionContext);
		  
	}
	
	
}
