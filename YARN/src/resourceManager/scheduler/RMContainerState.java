package resourceManager.scheduler;

public enum RMContainerState {
  NEW, 
  RESERVED, 
  ALLOCATED, 
  ACQUIRED, 
  RUNNING, 
  COMPLETED, 
  EXPIRED, 
  RELEASED, 
  KILLED
}
