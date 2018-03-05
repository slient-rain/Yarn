package resourceManager.scheduler;

import java.util.List;
/**
 * newlyLaunchedContainers + completedContainers
 * @author 无言的雨
 *
 */
public class UpdatedContainerInfo {
  private List<ContainerStatus> newlyLaunchedContainers;
  private List<ContainerStatus> completedContainers;
  
  public UpdatedContainerInfo() {
  }

  public UpdatedContainerInfo(List<ContainerStatus> newlyLaunchedContainers
      , List<ContainerStatus> completedContainers) {
    this.newlyLaunchedContainers = newlyLaunchedContainers;
    this.completedContainers = completedContainers;
  } 

  public List<ContainerStatus> getNewlyLaunchedContainers() {
    return this.newlyLaunchedContainers;
  }

  public List<ContainerStatus> getCompletedContainers() {
    return this.completedContainers;
  }
}
