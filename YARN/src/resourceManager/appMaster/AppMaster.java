package resourceManager.appMaster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import client.test.Main;

import com.sun.org.apache.regexp.internal.recompile;

import resourceManager.RMContext;
import resourceManager.applicationMasterLauncher.AMLauncherEvent;
import resourceManager.applicationMasterLauncher.AMLauncherEventType;
import resourceManager.scheduler.Allocation;
import resourceManager.scheduler.ApplicationAttemptId;
import resourceManager.scheduler.ContainerId;
import resourceManager.scheduler.Priority;
import resourceManager.scheduler.Resource;
import resourceManager.scheduler.ResourceRequest;

public class AppMaster implements  Runnable{
	private static final Logger LOG = LoggerFactory.getLogger(AppMaster.class);
	RMContext rmContext;
	boolean isStopped=false;
	ApplicationAttemptId applicationAttemptId;
	final int DEFAULTHEARTBEATINTERVAL=5000;
	public Object lock=new Object();
	public AppMaster(RMContext rmContext,ApplicationAttemptId applicationAttemptId) {
		this.rmContext=rmContext;
		this.applicationAttemptId=applicationAttemptId;
	}
	public void run() {
		while (!isStopped) {
			Resource resource=rmContext.getRMApps().get(applicationAttemptId.getApplicationId()).getApplicationSubmissionContext().getResource();
			ResourceRequest ask=new ResourceRequest(new Priority(1),ResourceRequest.ANY,resource ,1);
			Allocation allocation=rmContext.getScheduler().allocate(
					applicationAttemptId,
					new ArrayList<ResourceRequest>(Arrays.asList(ask)) , 
					new ArrayList<ContainerId>(), 
					new ArrayList<String>(), 
					new ArrayList<String>());
			if(allocation.getContainers().size()>0) {				
				System.out.println("appmaster 获取到资源:"+allocation.getContainers().get(0).toString());
				AMLauncherEvent event=new AMLauncherEvent(AMLauncherEventType.LAUNCH, allocation);
				rmContext.getDispatcher().getEventHandler().handle(event);
				break;
			}				
			try {
				synchronized (lock) {				
					LOG.info("appmaster 未获取到资源，重新进行allocate资源请求");
					lock.wait(DEFAULTHEARTBEATINTERVAL);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
