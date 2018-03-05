package resourceManager;


import java.util.List;

import protocol.protocolWritable.NodeHeartbeatResponse;

import dispatcher.core.Event;
import dispatcher.core.EventHandler;

import resourceManager.scheduler.ApplicationId;
import resourceManager.scheduler.ContainerId;
import resourceManager.scheduler.Node;
import resourceManager.scheduler.NodeId;
import resourceManager.scheduler.NodeState;
import resourceManager.scheduler.Resource;
import resourceManager.scheduler.UpdatedContainerInfo;


/**
 * 节点的一些端口等静态信息+该节点的一些心跳回复统计信息
 * 
 * Node managers information on available resources 
 * and other static information.
 *
 */
public interface RMNode extends EventHandler<RMNodeEvent> {

	/**
	 * 新加的方法，临时代替状态机处理containersToClean和finishedApplications
	 */
	public  void cleanupContainer(ContainerId containerId);
	public  void cleanupApplication(ApplicationId applicationId);
	
  /**
   * the node id of of this node.
   * @return the node id of this node.
   */
  public NodeId getNodeID();
  
  /**
   * the hostname of this node
   * @return hostname of this node
   */
  public String getHostName();
  
  /**
   * the command port for this node
   * @return command port for this node
   */
  public int getCommandPort();
  
  /**
   * the http port for this node
   * @return http port for this node
   */
  public int getHttpPort();


  /**
   * the ContainerManager address for this node.
   * @return the ContainerManager address for this node.
   */
  public String getNodeAddress();
  
  /**
   * the http-Address for this node.
   * @return the http-url address for this node
   */
  public String getHttpAddress();
  
  /**
   * the latest health report received from this node.
   * @return the latest health report received from this node.
   */
  public String getHealthReport();
  
  /**
   * the time of the latest health report received from this node.
   * @return the time of the latest health report received from this node.
   */
  public long getLastHealthReportTime();
  
  /**
   * the total available resource.
   * @return the total available resource.
   */
  public Resource getTotalCapability();
  
  /**
   * The rack name for this node manager.
   * @return the rack name.
   */
  public String getRackName();
  
  /**
   * the {@link Node} information for this node.
   * @return {@link Node} information for this node.
   */
  public Node getNode();
  
  public NodeState getState();

  public List<ContainerId> getContainersToCleanUp();

  public List<ApplicationId> getAppsToCleanup();

  /**
   * Update a {@link NodeHeartbeatResponse} with the list of containers and
   * applications to clean up for this node.
   * @param response the {@link NodeHeartbeatResponse} to update
   */
  public void updateNodeHeartbeatResponseForCleanup(NodeHeartbeatResponse response);

//  public NodeHeartbeatResponse getLastNodeHeartBeatResponse();
  
  /**
   * Get and clear the list of containerUpdates accumulated across NM
   * heartbeats.
   * newlyLaunchedContainers+completedContainers
   * @return containerUpdates accumulated across NM heartbeats.
   */
  public List<UpdatedContainerInfo> pullContainerUpdates();
}
