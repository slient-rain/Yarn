package resourceManager.resourceTrackerService.test;



import java.util.Map;
import java.util.Map.Entry;

import dispatcher.core.AsyncDispatcher;
import dispatcher.core.Dispatcher;
import dispatcher.core.EventHandler;
import recoverable.stateStore.RMStateStore;
import recoverable.stateStore.RMStateStore.RMState;
import resourceManager.RMApp;
import resourceManager.RMAppEvent;
import resourceManager.RMAppEventType;
import resourceManager.RMAppImpl;
import resourceManager.RMContext;
import resourceManager.RMContextImpl;
import resourceManager.ClientRMService.ClientRMService;
import resourceManager.resourceTrackerService.ResourceTrackerService;
import resourceManager.rmAppManager.RMAppManager;
import resourceManager.scheduler.ApplicationId;
import resourceManager.scheduler.Scheduler;
import resourceManager.scheduler.SchedulerEvent;
import resourceManager.scheduler.SchedulerEventType;

public class Main {
	public static void main(String[] args) throws Exception {
		Dispatcher dispatcher=new AsyncDispatcher();
		((AsyncDispatcher)dispatcher).init();
		((AsyncDispatcher)dispatcher).start();
		RMStateStore rmStateStore=new RMStateStore();
		rmStateStore.setDispatcher(dispatcher);
		Scheduler scheduler=new Scheduler();
		dispatcher.register(SchedulerEventType.class, scheduler);
		RMContext context=new RMContextImpl(dispatcher,rmStateStore);
		context.setScheduler(scheduler);
		RMAppManager rAppManager=new RMAppManager(context);
		ClientRMService clientRMService=new ClientRMService("ClientRMService",context,rAppManager);
		clientRMService.init();
		clientRMService.start();
		ResourceTrackerService resourceTrackerService=new ResourceTrackerService(context);
		resourceTrackerService.init();
		resourceTrackerService.start();
//		ClientRMService clientRMService=new ClientRMService("ClientRMService",context,rAppManager);
//		clientRMService.init();
//		clientRMService.start();
//		Thread.sleep(5000);
//		RMAppEvent event=new RMAppEvent(new ApplicationId(1, 20170531), RMAppEventType.APP_SAVED);
//		dispatcher.getEventHandler().handle(event);
//		
//		((RMAppImpl)context.getRMApps().get(new ApplicationId(32,20170530))).recover(rmStateStore.getRMState());
		
	}
	
}
