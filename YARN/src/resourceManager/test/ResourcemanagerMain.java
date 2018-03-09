package resourceManager.test;

import recoverable.stateStore.RMStateStore;
import resourceManager.RMContext;
import resourceManager.RMContextImpl;
import resourceManager.ClientRMService.ClientRMService;
import resourceManager.applicationMasterLauncher.AMLauncherEventType;
import resourceManager.applicationMasterLauncher.ApplicationMasterLauncher;
import resourceManager.resourceTrackerService.ResourceTrackerService;
import resourceManager.rmAppManager.RMAppManager;
import resourceManager.scheduler.Scheduler;
import resourceManager.scheduler.SchedulerEventType;
import dispatcher.core.AsyncDispatcher;
import dispatcher.core.Dispatcher;

public class ResourcemanagerMain {
	public static void main(String[] args) {

		Dispatcher dispatcher = new AsyncDispatcher();
		((AsyncDispatcher) dispatcher).init();
		((AsyncDispatcher) dispatcher).start();
		RMStateStore rmStateStore = new RMStateStore();
		rmStateStore.setDispatcher(dispatcher);

		Scheduler scheduler = new Scheduler();
		dispatcher.register(SchedulerEventType.class, scheduler);
		RMContext context = new RMContextImpl(dispatcher, rmStateStore);
		context.setScheduler(scheduler);

		ApplicationMasterLauncher applicationMasterLauncher = new ApplicationMasterLauncher(
				context);
		dispatcher.register(AMLauncherEventType.class,
				applicationMasterLauncher);
		applicationMasterLauncher.init();
		applicationMasterLauncher.start();

		RMAppManager rAppManager = new RMAppManager(context);
		ClientRMService clientRMService = new ClientRMService(
				"ClientRMService", context, rAppManager);
		clientRMService.init();
		clientRMService.start();

		ResourceTrackerService resourceTrackerService = new ResourceTrackerService(
				context);
		resourceTrackerService.init();
		resourceTrackerService.start();
		// RMApp app=new RMAppImpl(
		// new ApplicationId(1, 20170531),
		// null,
		// "application state machine test",
		// "user test",
		// "queue test",
		// null,
		// 20170531
		// );
		// AsyncDispatcher dispatcher=new AsyncDispatcher();
		// dispatcher.init();
		// dispatcher.start();
		// dispatcher.register(RMAppEventType.class, app);
		// RMAppEvent event=new RMAppEvent(new ApplicationId(1, 20170531),
		// RMAppEventType.START);
		// dispatcher.getEventHandler().handle(event);
		// event=new RMAppEvent(new ApplicationId(1, 20170531),
		// RMAppEventType.APP_SAVED);
		// dispatcher.getEventHandler().handle(event);
		// event=new RMAppEvent(new ApplicationId(1, 20170531),
		// RMAppEventType.APP_ACCEPTED);
		// dispatcher.getEventHandler().handle(event);
	}
}
