/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package resourceManager;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import protocol.protocolWritable.NodeHeartbeatResponse;

import resourceManager.scheduler.ApplicationId;
import resourceManager.scheduler.ContainerId;
import resourceManager.scheduler.ContainerState;
import resourceManager.scheduler.ContainerStatus;
import resourceManager.scheduler.Node;
import resourceManager.scheduler.NodeAddedSchedulerEvent;
import resourceManager.scheduler.NodeId;
import resourceManager.scheduler.NodeState;
import resourceManager.scheduler.NodeUpdateSchedulerEvent;
import resourceManager.scheduler.Resource;
import resourceManager.scheduler.UpdatedContainerInfo;
import state.InvalidStateTransitonException;
import state.MultipleArcTransition;
import state.SingleArcTransition;
import state.StateMachine;
import state.StateMachineFactory;

import dispatcher.core.EventHandler;

/**
 * 用于封装与NodeManager通信的的node信息和container、appliaction信息 This class is used to keep
 * track of all the applications/containers running on a node.
 *
 */
@SuppressWarnings("unchecked")
public class RMNodeImpl implements RMNode {

	private static final Logger LOG = LoggerFactory.getLogger(RMNodeImpl.class);
	private final ReadLock readLock;
	private final WriteLock writeLock;

	private final ConcurrentLinkedQueue<UpdatedContainerInfo> nodeUpdateQueue;
	/* set of containers that need to be cleaned */
	// private final Set<ContainerId> containersToClean = new
	// TreeSet<ContainerId>(new ContainerIdComparator());
	private final Set<ContainerId> containersToClean = new TreeSet<ContainerId>();

	/* the list of applications that have finished and need to be purged */
	private final List<ApplicationId> finishedApplications = new ArrayList<ApplicationId>();

	private volatile boolean nextHeartBeat = true;

	private final NodeId nodeId;
	private final RMContext context;
	private final String hostName;
	private final int commandPort;
	private final int httpPort;
	private final String nodeAddress; // The containerManager address
	private final String httpAddress;
	private final Resource totalCapability;
	/**
	 * 网络拓扑结构信息
	 */
	private final Node node;

	private String healthReport;
	private long lastHealthReportTime;

	public RMNodeImpl(NodeId nodeId, RMContext context, String hostName,
			int cmPort, int httpPort, Node node, Resource capability) {
		this.nodeId = nodeId;
		this.context = context;
		this.hostName = hostName;
		this.commandPort = cmPort;
		this.httpPort = httpPort;
		this.totalCapability = capability;
		this.nodeAddress = hostName + ":" + cmPort;
		this.httpAddress = hostName + ":" + httpPort;
		this.node = node;
		this.healthReport = "Healthy";
		this.lastHealthReportTime = System.currentTimeMillis();

		// this.latestNodeHeartBeatResponse.setResponseId(0);

		ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
		this.readLock = lock.readLock();
		this.writeLock = lock.writeLock();

		this.stateMachine = stateMachineFactory.make(this);

		this.nodeUpdateQueue = new ConcurrentLinkedQueue<UpdatedContainerInfo>();
	}

	/**
	 * 新加的方法，临时代替状态机处理containersToClean和finishedApplications
	 */
	@Override
	public synchronized void cleanupContainer(ContainerId containerId) {
		this.containersToClean.add(containerId);
	}

	@Override
	public synchronized void cleanupApplication(ApplicationId applicationId) {
		this.finishedApplications.add(applicationId);
	}

	@Override
	public String toString() {
		return this.nodeId.toString();
	}

	@Override
	public String getHostName() {
		return hostName;
	}

	@Override
	public int getCommandPort() {
		return commandPort;
	}

	@Override
	public int getHttpPort() {
		return httpPort;
	}

	@Override
	public NodeId getNodeID() {
		return this.nodeId;
	}

	@Override
	public String getNodeAddress() {
		return this.nodeAddress;
	}

	@Override
	public String getHttpAddress() {
		return this.httpAddress;
	}

	@Override
	public Resource getTotalCapability() {
		return this.totalCapability;
	}

	@Override
	/**
	 * 待实现
	 */
	public String getRackName() {
		// return node.getNetworkLocation();
		return null;
	}

	@Override
	public Node getNode() {
		return this.node;
	}

	@Override
	public String getHealthReport() {
		this.readLock.lock();

		try {
			return this.healthReport;
		} finally {
			this.readLock.unlock();
		}
	}

	public void setHealthReport(String healthReport) {
		this.writeLock.lock();

		try {
			this.healthReport = healthReport;
		} finally {
			this.writeLock.unlock();
		}
	}

	public void setLastHealthReportTime(long lastHealthReportTime) {
		this.writeLock.lock();

		try {
			this.lastHealthReportTime = lastHealthReportTime;
		} finally {
			this.writeLock.unlock();
		}
	}

	@Override
	public long getLastHealthReportTime() {
		this.readLock.lock();

		try {
			return this.lastHealthReportTime;
		} finally {
			this.readLock.unlock();
		}
	}

	@Override
	public NodeState getState() {
		this.readLock.lock();

		try {
			return this.stateMachine.getCurrentState();
		} finally {
			this.readLock.unlock();
		}
	}

	@Override
	public List<ApplicationId> getAppsToCleanup() {
		this.readLock.lock();

		try {
			return new ArrayList<ApplicationId>(this.finishedApplications);
		} finally {
			this.readLock.unlock();
		}

	}

	@Override
	public List<ContainerId> getContainersToCleanUp() {

		this.readLock.lock();

		try {
			return new ArrayList<ContainerId>(this.containersToClean);
		} finally {
			this.readLock.unlock();
		}
	};

	/**
	 * 所有运行在该节点上的container nodeheartbeat 汇报为状态为ContainerState.RUNNING的container
	 */
	private final Map<ContainerId, ContainerStatus> justLaunchedContainers = new HashMap<ContainerId, ContainerStatus>();

	@Override
	public List<UpdatedContainerInfo> pullContainerUpdates() {
		List<UpdatedContainerInfo> latestContainerInfoList = new ArrayList<UpdatedContainerInfo>();
		while (nodeUpdateQueue.peek() != null) {
			latestContainerInfoList.add(nodeUpdateQueue.poll());
		}
		this.nextHeartBeat = true;
		return latestContainerInfoList;
	}

	public void setNextHeartBeat(boolean nextHeartBeat) {
		this.nextHeartBeat = nextHeartBeat;
	}

	public int getQueueSize() {
		return nodeUpdateQueue.size();
	}

	// private NodeHeartbeatResponse latestNodeHeartBeatResponse = recordFactory
	// .newRecordInstance(NodeHeartbeatResponse.class);

	private static final StateMachineFactory<RMNodeImpl, NodeState, RMNodeEventType, RMNodeEvent> stateMachineFactory = new StateMachineFactory<RMNodeImpl, NodeState, RMNodeEventType, RMNodeEvent>(
			NodeState.NEW)

			// Transitions from NEW state
			.addTransition(NodeState.NEW, NodeState.RUNNING,
					RMNodeEventType.STARTED, new AddNodeTransition())

			// Transitions from RUNNING state
			.addTransition(NodeState.RUNNING,
					EnumSet.of(NodeState.RUNNING, NodeState.UNHEALTHY),
					RMNodeEventType.STATUS_UPDATE,
					new StatusUpdateWhenHealthyTransition())
			.addTransition(NodeState.RUNNING, NodeState.DECOMMISSIONED,
					RMNodeEventType.DECOMMISSION,
					new DeactivateNodeTransition(NodeState.DECOMMISSIONED))
			.addTransition(NodeState.RUNNING, NodeState.LOST,
					RMNodeEventType.EXPIRE,
					new DeactivateNodeTransition(NodeState.LOST))
			.addTransition(NodeState.RUNNING, NodeState.REBOOTED,
					RMNodeEventType.REBOOTING,
					new DeactivateNodeTransition(NodeState.REBOOTED))
			.addTransition(NodeState.RUNNING, NodeState.RUNNING,
					RMNodeEventType.CLEANUP_APP, new CleanUpAppTransition())
			.addTransition(NodeState.RUNNING, NodeState.RUNNING,
					RMNodeEventType.CLEANUP_CONTAINER,
					new CleanUpContainerTransition())
			.addTransition(NodeState.RUNNING, NodeState.RUNNING,
					RMNodeEventType.RECONNECTED, new ReconnectNodeTransition())

			// Transitions from UNHEALTHY state
			.addTransition(NodeState.UNHEALTHY,
					EnumSet.of(NodeState.UNHEALTHY, NodeState.RUNNING),
					RMNodeEventType.STATUS_UPDATE,
					new StatusUpdateWhenUnHealthyTransition())
			.addTransition(NodeState.UNHEALTHY, NodeState.DECOMMISSIONED,
					RMNodeEventType.DECOMMISSION,
					new DeactivateNodeTransition(NodeState.DECOMMISSIONED))
			.addTransition(NodeState.UNHEALTHY, NodeState.LOST,
					RMNodeEventType.EXPIRE,
					new DeactivateNodeTransition(NodeState.LOST))
			.addTransition(NodeState.UNHEALTHY, NodeState.REBOOTED,
					RMNodeEventType.REBOOTING,
					new DeactivateNodeTransition(NodeState.REBOOTED))
			.addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
					RMNodeEventType.RECONNECTED, new ReconnectNodeTransition())
			.addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
					RMNodeEventType.CLEANUP_APP, new CleanUpAppTransition())
			.addTransition(NodeState.UNHEALTHY, NodeState.UNHEALTHY,
					RMNodeEventType.CLEANUP_CONTAINER,
					new CleanUpContainerTransition())

			// create the topology tables
			.installTopology();

	private final StateMachine<NodeState, RMNodeEventType, RMNodeEvent> stateMachine;

	@Override
	public void updateNodeHeartbeatResponseForCleanup(
			NodeHeartbeatResponse response) {
		this.writeLock.lock();

		try {
			response.addAllContainersToCleanup(new ArrayList<ContainerId>(
					this.containersToClean));
			response.addAllApplicationsToCleanup(this.finishedApplications);
			this.containersToClean.clear();
			this.finishedApplications.clear();
		} finally {
			this.writeLock.unlock();
		}
	};

	@Override
	public void handle(RMNodeEvent event) {

		try {
			writeLock.lock();
			NodeId nodeid = event.getNodeId();
			//每个rmnode只处理自己的event
			if (this.nodeId.compareTo(nodeid) == 0) {
				LOG.debug("Processing " + event.getNodeId() + " of type "
						+ event.getType());
				NodeState oldState = getState();
				try {
					stateMachine.doTransition(event.getType(), event);
				} catch (InvalidStateTransitonException e) {
					LOG.error("Can't handle this event at current state", e);
					LOG.error("Invalid event " + event.getType() + " on Node  "
							+ this.nodeId);
				}
				if (oldState != getState()) {
					LOG.info(nodeId + " Node Transitioned from " + oldState
							+ " to " + getState());
				}
			}
		}

		finally {
			writeLock.unlock();
		}
	}

	public static class AddNodeTransition implements
			SingleArcTransition<RMNodeImpl, RMNodeEvent> {

		@Override
		public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
			// Inform the scheduler

			rmNode.context.getDispatcher().getEventHandler()
					.handle(new NodeAddedSchedulerEvent(rmNode));
		}
	}

	public static class ReconnectNodeTransition implements
			SingleArcTransition<RMNodeImpl, RMNodeEvent> {

		@Override
		public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
			// // Kill containers since node is rejoining.
			// rmNode.nodeUpdateQueue.clear();
			// rmNode.context.getDispatcher().getEventHandler().handle(
			// new NodeRemovedSchedulerEvent(rmNode));
			//
			// RMNode newNode =
			// ((RMNodeReconnectEvent)event).getReconnectedNode();
			// if
			// (rmNode.getTotalCapability().equals(newNode.getTotalCapability())
			// && rmNode.getHttpPort() == newNode.getHttpPort()) {
			// // Reset heartbeat ID since node just restarted.
			// rmNode.getLastNodeHeartBeatResponse().setResponseId(0);
			// if (rmNode.getState() != NodeState.UNHEALTHY) {
			// // Only add new node if old state is not UNHEALTHY
			// rmNode.context.getDispatcher().getEventHandler().handle(
			// new NodeAddedSchedulerEvent(rmNode));
			// }
			// } else {
			// // Reconnected node differs, so replace old node and start new
			// node
			// switch (rmNode.getState()) {
			// case RUNNING:
			// ClusterMetrics.getMetrics().decrNumActiveNodes();
			// break;
			// case UNHEALTHY:
			// ClusterMetrics.getMetrics().decrNumUnhealthyNMs();
			// break;
			// }
			// rmNode.context.getRMNodes().put(newNode.getNodeID(), newNode);
			// rmNode.context.getDispatcher().getEventHandler().handle(
			// new RMNodeEvent(newNode.getNodeID(), RMNodeEventType.STARTED));
			// }
		}
	}

	public static class CleanUpAppTransition implements
			SingleArcTransition<RMNodeImpl, RMNodeEvent> {

		@Override
		public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
			// rmNode.finishedApplications.add(((
			// RMNodeCleanAppEvent) event).getAppId());
		}
	}

	public static class CleanUpContainerTransition implements
			SingleArcTransition<RMNodeImpl, RMNodeEvent> {

		@Override
		public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
			// rmNode.containersToClean.add(((
			// RMNodeCleanContainerEvent) event).getContainerId());
		}
	}

	public static class DeactivateNodeTransition implements
			SingleArcTransition<RMNodeImpl, RMNodeEvent> {

		private final NodeState finalState;

		public DeactivateNodeTransition(NodeState finalState) {
			this.finalState = finalState;
		}

		@Override
		public void transition(RMNodeImpl rmNode, RMNodeEvent event) {
			// // Inform the scheduler
			// rmNode.nodeUpdateQueue.clear();
			// // If the current state is NodeState.UNHEALTHY
			// // Then node is already been removed from the
			// // Scheduler
			// NodeState initialState = rmNode.getState();
			// if (!initialState.equals(NodeState.UNHEALTHY)) {
			// rmNode.context.getDispatcher().getEventHandler()
			// .handle(new NodeRemovedSchedulerEvent(rmNode));
			// }
			// rmNode.context.getDispatcher().getEventHandler().handle(
			// new NodesListManagerEvent(
			// NodesListManagerEventType.NODE_UNUSABLE, rmNode));
			//
			// // Deactivate the node
			// rmNode.context.getRMNodes().remove(rmNode.nodeId);
			// LOG.info("Deactivating Node " + rmNode.nodeId + " as it is now "
			// + finalState);
			// rmNode.context.getInactiveRMNodes().put(rmNode.nodeId.getHost(),
			// rmNode);
			//
			// //Update the metrics
			// rmNode.updateMetricsForDeactivatedNode(initialState, finalState);
		}
	}

	public static class StatusUpdateWhenHealthyTransition implements
			MultipleArcTransition<RMNodeImpl, RMNodeEvent, NodeState> {
		@Override
		public NodeState transition(RMNodeImpl rmNode, RMNodeEvent event) {
			RMNodeStatusEvent statusEvent = (RMNodeStatusEvent) event;
			List<ContainerStatus> newlyLaunchedContainers = new ArrayList<ContainerStatus>();
			List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
			for (ContainerStatus remoteContainer : statusEvent.getContainers()) {
				ContainerId containerId = remoteContainer.getContainerId();
				if (rmNode.containersToClean.contains(containerId)) {
					LOG.info("Container " + containerId
							+ " already scheduled for "
							+ "cleanup, no further processing");
					continue;
				}
				if (rmNode.finishedApplications.contains(containerId
						.getApplicationAttemptId().getApplicationId())) {
					LOG.info("Container "
							+ containerId
							+ " belongs to an application that is already killed,"
							+ " no further processing");
					continue;
				}

				// Process running containers
				if (remoteContainer.getState() == ContainerState.RUNNING) {
					if (!rmNode.justLaunchedContainers.containsKey(containerId)) {
						// Just launched container. RM knows about it the first
						// time.
						rmNode.justLaunchedContainers.put(containerId,
								remoteContainer);
						newlyLaunchedContainers.add(remoteContainer);
					}
				} else {
					// A finished container
					rmNode.justLaunchedContainers.remove(containerId);
					completedContainers.add(remoteContainer);
				}
			}
			if (newlyLaunchedContainers.size() != 0
					|| completedContainers.size() != 0) {
				rmNode.nodeUpdateQueue.add(new UpdatedContainerInfo(
						newlyLaunchedContainers, completedContainers));
			}
			if (rmNode.nextHeartBeat) {
				rmNode.nextHeartBeat = false;
				rmNode.context.getDispatcher().getEventHandler()
						.handle(new NodeUpdateSchedulerEvent(rmNode));
			}

			return NodeState.RUNNING;
		}
	}

	public static class StatusUpdateWhenUnHealthyTransition implements
			MultipleArcTransition<RMNodeImpl, RMNodeEvent, NodeState> {

		@Override
		public NodeState transition(RMNodeImpl rmNode, RMNodeEvent event) {
			// RMNodeStatusEvent statusEvent = (RMNodeStatusEvent) event;
			//
			// // Switch the last heartbeatresponse.
			// rmNode.latestNodeHeartBeatResponse =
			// statusEvent.getLatestResponse();
			// NodeHealthStatus remoteNodeHealthStatus =
			// statusEvent.getNodeHealthStatus();
			// rmNode.setHealthReport(remoteNodeHealthStatus.getHealthReport());
			// rmNode.setLastHealthReportTime(
			// remoteNodeHealthStatus.getLastHealthReportTime());
			// if (remoteNodeHealthStatus.getIsNodeHealthy()) {
			// rmNode.context.getDispatcher().getEventHandler().handle(
			// new NodeAddedSchedulerEvent(rmNode));
			// rmNode.context.getDispatcher().getEventHandler().handle(
			// new NodesListManagerEvent(
			// NodesListManagerEventType.NODE_USABLE, rmNode));
			// // ??? how about updating metrics before notifying to ensure that
			// // notifiers get update metadata because they will very likely
			// query it
			// // upon notification
			// // Update metrics
			// rmNode.updateMetricsForRejoinedNode(NodeState.UNHEALTHY);
			// return NodeState.RUNNING;
			// }
			//
			return NodeState.UNHEALTHY;
		}
	}

}
