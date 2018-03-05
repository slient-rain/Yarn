package resourceManager.scheduler;

import java.util.List;

import dispatcher.core.EventHandler;
import service.AbstractService;

public interface YarnScheduler extends EventHandler<SchedulerEvent> {
	/**
	 * The main api between the ApplicationMaster and the Scheduler.
	 * The ApplicationMaster is updating his future resource requirements
	 * and may release containers he doens't need.
	 * 
	 * @param appAttemptId
	 * @param ask
	 * @param release
	 * @param blacklistAdditions 
	 * @param blacklistRemovals 
	 * @return the {@link Allocation} for the application
	 */
	Allocation 
	allocate(ApplicationAttemptId appAttemptId, 
			List<ResourceRequest> ask,
			List<ContainerId> release, 
			List<String> blacklistAdditions, 
			List<String> blacklistRemovals);
}
