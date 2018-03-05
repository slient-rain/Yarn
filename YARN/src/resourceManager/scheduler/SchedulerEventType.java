package resourceManager.scheduler;

public enum SchedulerEventType {
	// Source: Node
	  NODE_ADDED,
	  NODE_REMOVED,
	  NODE_UPDATE,
	  
	  // Source: App
	  APP_ADDED,
	  APP_REMOVED,

	  // Source: ContainerAllocationExpirer
	  CONTAINER_EXPIRED
}
