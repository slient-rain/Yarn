package resourceManager.scheduler;

import dispatcher.core.AbstractEvent;

public class SchedulerEvent extends AbstractEvent<SchedulerEventType> {
	public SchedulerEvent(SchedulerEventType type) {
	    super(type);
	  }
}
