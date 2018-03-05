package client;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import nodeManager.resourceLocalizationService.LocalResourceVisibility;

import protocol.protocolWritable.ApplicationSubmissionContext;
import protocol.protocolWritable.ApplicationSubmissionContext.LocalResource;
import protocol.protocolWritable.ApplicationSubmissionContext.URL;
import resourceManager.scheduler.Priority;
import resourceManager.scheduler.Resource;

public class Client {
	YarnClient yarnClient;
	public Client() {
		yarnClient=new YarnClientImpl();
	}
	public static void main(String[] args) {
		Client client = new Client();
		client.yarnClient.init();
		client.yarnClient.start();
		YarnClientApplication application=client.yarnClient.createApplication();
		ApplicationSubmissionContext submissionContext=application.getApplicationSubmissionContext();
		submissionContext.setApplicationName("测试应用");
		submissionContext.setCommand(Arrays.asList("java -jar ..."));
		String applicationId=application.getNewApplicationResponse().getApplicationId().toString();
		Map<String,ApplicationSubmissionContext.LocalResource> map=new HashMap<String, ApplicationSubmissionContext.LocalResource>();
		for(int i=0; i<2; i++){
			String file="cgroup"+i+".jar";
			URL resource=submissionContext.new URL("192.168.1.59",8080,"AppStore/Download?applicationId="+applicationId+"&file="+file);
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
