package resourceManager.applicationMasterLauncher.test;

import java.util.ArrayList;
import java.util.Arrays;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import protocol.protocol.ContainerManagementProtocol;
import protocol.protocolWritable.ApplicationSubmissionContext;
import protocol.protocolWritable.ContainerLaunchContext;
import protocol.protocolWritable.StartContainerRequest;
import protocol.protocolWritable.StartContainersRequest;
import protocol.protocolWritable.StartContainersResponse;
import protocol.protocolWritable.ApplicationSubmissionContext.LocalResource;
import protocol.protocolWritable.ApplicationSubmissionContext.URL;
import dispatcher.core.AsyncDispatcher;
import dispatcher.core.Dispatcher;
import recoverable.stateStore.RMStateStore;
import resourceManager.RMApp;
import resourceManager.RMContext;
import resourceManager.RMContextImpl;
import resourceManager.applicationMasterLauncher.AMLauncher;
import resourceManager.applicationMasterLauncher.AMLauncherEvent;
import resourceManager.applicationMasterLauncher.AMLauncherEventType;
import resourceManager.applicationMasterLauncher.ApplicationMasterLauncher;
import resourceManager.scheduler.Allocation;
import resourceManager.scheduler.ApplicationAttemptId;
import resourceManager.scheduler.ApplicationId;
import resourceManager.scheduler.Container;
import resourceManager.scheduler.ContainerId;
import resourceManager.scheduler.NodeId;
import resourceManager.scheduler.Priority;
import resourceManager.scheduler.Resource;
import resourceManager.scheduler.ResourceRequest;
import resourceManager.scheduler.Scheduler;
import resourceManager.scheduler.SchedulerEventType;

public class Main {
	public static void main(String[] args) {
		ApplicationSubmissionContext submissionContext=new ApplicationSubmissionContext();
		submissionContext.setApplicationName("测试应用");
		submissionContext.setCommand(Arrays.asList("java -jar ","task0.jar"));
		Map<String,ApplicationSubmissionContext.LocalResource> map=new HashMap<String, ApplicationSubmissionContext.LocalResource>();
		for(int i=0; i<1; i++){
			String file="task"+i+".jar";
			URL resource=submissionContext.new URL("192.168.2.134",8080,"AppStore/Download?applicationId="+new ApplicationId(1,2001)+"&file="+file);
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
		

		ApplicationAttemptId  appid=new ApplicationAttemptId(new ApplicationId(1,2001),1);
		ContainerId containerId=new ContainerId(appid,1);
		NodeId nodeid=new NodeId("localhost",8001);
		Container container=new Container(containerId, nodeid, null, null, null);
		List<Container> containers=new ArrayList();
		containers.add(container);
		Allocation allocation=new Allocation(containers,submissionContext.getResource());
		
		AMLauncher launcher=new AMLauncher();
		ContainerManagementProtocol containerMgrProxy=launcher.getContainerMgrProxy(container);
		StartContainersRequest requests=new StartContainersRequest();
		List<StartContainerRequest> list=new ArrayList<StartContainerRequest>();
		StartContainerRequest request=new StartContainerRequest();
		request.setContainerLaunchContext(new ContainerLaunchContext(
				submissionContext.getLocalResouces(),
					new HashMap<String, String>(),
					submissionContext.getCommand(),
					submissionContext.getResource(),
					containerId,
					appid.getApplicationId(),
					submissionContext.getUser()
					));
			list.add(request);
		requests.setRequests(list);
		StartContainersResponse response=containerMgrProxy.startContainers(requests);
		
	}
}
