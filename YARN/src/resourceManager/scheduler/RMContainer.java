package resourceManager.scheduler;

import dispatcher.core.EventHandler;


/**
 * Represents the ResourceManager's view of an application container. See 
 * {@link RMContainerImpl} for an implementation. Containers may be in one
 * of several states, given in {@link RMContainerState}. An RMContainer
 * instance may exist even if there is no actual running container, such as 
 * when resources are being reserved to fill space for a future container 
 * allocation.
 */
public interface RMContainer extends EventHandler<RMContainerEvent> {

  ContainerId getContainerId();

  ApplicationAttemptId getApplicationAttemptId();

  RMContainerState getState();

  Container getContainer();

  Resource getReservedResource();

  NodeId getReservedNode();
  
  Priority getReservedPriority();

}
