package resourceManager.scheduler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import resourceManager.RMNode;

import com.sun.org.apache.bcel.internal.generic.NEW;

public class Scheduler implements ResourceScheduler {

	private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
	private Map<NodeId, SchedulerNode> nodes = new ConcurrentHashMap<NodeId, SchedulerNode>();
	// Use ConcurrentSkipListMap because applications need to be ordered
	protected Map<ApplicationAttemptId, SchedulerApp> applications = new ConcurrentSkipListMap<ApplicationAttemptId, SchedulerApp>();
	private Resource clusterResource = new Resource();
	private Resource usedResource = new Resource();

	private boolean usePortForNodeName = true;
	private final static Container[] EMPTY_CONTAINER_ARRAY = new Container[] {};
	private final static List<Container> EMPTY_CONTAINER_LIST = Arrays
			.asList(EMPTY_CONTAINER_ARRAY);
	private static final Allocation EMPTY_ALLOCATION = new Allocation(
			EMPTY_CONTAINER_LIST, createResource(0));
	private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();
	private Resource minimumAllocation;
	private Resource maximumAllocation;

	public Scheduler() {
		/**
		 * 最小最大分配资源限制
		 */
		minimumAllocation = new Resource(50, 1);
		maximumAllocation = new Resource(1000, 4);
	}

	@Override
	public Allocation allocate(ApplicationAttemptId applicationAttemptId,
			List<ResourceRequest> ask, List<ContainerId> release,
			List<String> blacklistAdditions, List<String> blacklistRemovals) {
		SchedulerApp application = getApplication(applicationAttemptId);
		if (application == null) {
			LOG.error("Calling allocate on removed "
					+ "or non existant application " + applicationAttemptId);
			return EMPTY_ALLOCATION;
		}

		// Sanity check
		normalizeRequests(ask, resourceCalculator, clusterResource,
				minimumAllocation, maximumAllocation);

		// Release containers
		for (ContainerId releasedContainer : release) {
			RMContainer rmContainer = getRMContainer(releasedContainer);
			if (rmContainer == null) {
				// RMAuditLogger.logFailure(application.getUser(),
				// AuditConstants.RELEASE_CONTAINER,
				LOG.warn(
						"Unauthorized access or invalid container",
						"FifoScheduler",
						"Trying to release container not owned by app or with invalid id",
						application.getApplicationId(), releasedContainer);
			}
			containerCompleted(rmContainer, null, null);
			// SchedulerUtils.createAbnormalContainerStatus(
			// releasedContainer,
			// SchedulerUtils.RELEASED_CONTAINER),
			// RMContainerEventType.RELEASED
		}

		synchronized (application) {

			// make sure we aren't stopping/removing the application
			// when the allocate comes in
			if (application.isStopped()) {
				LOG.info("Calling allocate on a stopped " + "application "
						+ applicationAttemptId);
				return EMPTY_ALLOCATION;
			}

			if (!ask.isEmpty()) {
				LOG.debug("allocate: pre-update" + " applicationId="
						+ applicationAttemptId + " application=" + application);
				application.showRequests();

				// Update application requests
				application.updateResourceRequests(ask);
				LOG.debug("util check: 当前请求量："
						+ (application.getResourceRequest(new Priority(1),
								ResourceRequest.ANY).getCapability().toString()));
				LOG.debug("allocate: post-update" + " applicationId="
						+ applicationAttemptId + " application=" + application);
				application.showRequests();

				LOG.debug("allocate:" + " applicationId="
						+ applicationAttemptId + " #ask=" + ask.size());
			}

			// application.updateBlacklist(blacklistAdditions,
			// blacklistRemovals);

			return new Allocation(application.pullNewlyAllocatedContainers(),
					application.getHeadroom());
		}
	}

	@Override
	public void handle(SchedulerEvent event) {
		LOG.debug("Processing scheduler of type " + event.getType());
		switch (event.getType()) {
		case NODE_ADDED: {
			NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent) event;
			addNode(nodeAddedEvent.getAddedRMNode());
		}
			break;
		case NODE_REMOVED: {
			NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent) event;
			removeNode(nodeRemovedEvent.getRemovedRMNode());
		}
			break;
		case NODE_UPDATE: {
			NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent) event;
			nodeUpdate(nodeUpdatedEvent.getRMNode());
		}
			break;
		case APP_ADDED: {
			AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
			addApplication(appAddedEvent.getApplicationAttemptId(),
					appAddedEvent.getUser());
		}
			break;
		case APP_REMOVED: {
			AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent) event;
			try {
				doneApplication(appRemovedEvent.getApplicationAttemptID(),
						appRemovedEvent.getFinalAttemptState());
			} catch (IOException ie) {
				LOG.error(
						"Unable to remove application "
								+ appRemovedEvent.getApplicationAttemptID(), ie);
			}
		}
			break;
		case CONTAINER_EXPIRED: {
			ContainerExpiredSchedulerEvent containerExpiredEvent = (ContainerExpiredSchedulerEvent) event;
			ContainerId containerid = containerExpiredEvent.getContainerId();
			containerCompleted(getRMContainer(containerid), null, null);// ,
			// SchedulerUtils.createAbnormalContainerStatus(
			// containerid,
			// SchedulerUtils.EXPIRED_CONTAINER),
			// RMContainerEventType.EXPIRE
		}
			break;
		default:
			LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
		}
	}

	private synchronized void addApplication(ApplicationAttemptId appAttemptId,
			String user) {
		// TODO: Fix store
		SchedulerApp schedulerApp = new SchedulerApp(appAttemptId);
		// applications is a ordered map
		applications.put(appAttemptId, schedulerApp);
		LOG.info("Application Submission: " + appAttemptId.getApplicationId()
				+ " from " + user + ", currently active: "
				+ applications.size());
	}

	private synchronized void doneApplication(
			ApplicationAttemptId applicationAttemptId,
			RMAppAttemptState rmAppAttemptFinalState) throws IOException {
		SchedulerApp application = getApplication(applicationAttemptId);
		if (application == null) {
			throw new IOException("Unknown application " + applicationAttemptId
					+ " has completed!");
		}

		// Kill all 'live' containers
		for (RMContainer container : application.getLiveContainers()) {
			containerCompleted(container, null, null);// ,
			// SchedulerUtils.createAbnormalContainerStatus(container.getContainerId(),
			// SchedulerUtils.COMPLETED_APPLICATION),
			// RMContainerEventType.KILL

			/**
			 * 新加的临时处理逻辑 add application to the finishedApplication list of
			 * rmNode
			 */
			SchedulerNode node = getNode(container.getContainer().getNodeId());
			node.getRMNode().cleanupApplication(
					getApplication(applicationAttemptId).getApplicationId());
		}

		// // Inform the activeUsersManager
		// synchronized (application) {
		// activeUsersManager.deactivateApplication(
		// application.getUser(), application.getApplicationId());
		// }

		// Clean up pending requests, metrics etc.
		application.stop(rmAppAttemptFinalState);

		// Remove the application
		applications.remove(applicationAttemptId);
	}

	/**
	 * 更新相关数据结构，通知节点释放Container 将container 加入containersToCleanUp列表中
	 * 同时直将container的资源加入到可用资源列表中
	 */
	private synchronized void containerCompleted(RMContainer rmContainer,
			ContainerStatus containerStatus, RMContainerEventType event) {
		if (rmContainer == null) {
			LOG.info("Null container completed...");
			return;
		}

		// Get the application for the finished container
		Container container = rmContainer.getContainer();
		ApplicationAttemptId applicationAttemptId = container.getId()
				.getApplicationAttemptId();
		SchedulerApp application = getApplication(applicationAttemptId);

		// Get the node on which the container was allocated
		SchedulerNode node = getNode(container.getNodeId());

		if (application == null) {
			LOG.info("Unknown application: " + applicationAttemptId
					+ " released container " + container.getId() + " on node: "
					+ node + " with event: " + event);
			return;
		}

		// Inform the application
		application.containerCompleted(rmContainer, containerStatus, event);

		// add container to the containersToClean list of rmNode
		node.getRMNode().cleanupContainer(rmContainer.getContainerId());

		// Inform the node
		node.releaseContainer(container);

		// Update total usage
		subtractFrom(usedResource, container.getResource());

		LOG.info("Application " + applicationAttemptId + " released container "
				+ container.getId() + " on node: " + node + " with event: "
				+ event);

	}

	private synchronized void addNode(RMNode nodeManager) {
		this.nodes.put(nodeManager.getNodeID(), new SchedulerNode(nodeManager,
				usePortForNodeName));
		addTo(clusterResource, nodeManager.getTotalCapability());
	}

	private synchronized void removeNode(RMNode nodeInfo) {
		SchedulerNode node = getNode(nodeInfo.getNodeID());
		if (node == null) {
			return;
		}
		// Kill running containers
		for (RMContainer container : node.getRunningContainers()) {
			containerCompleted(container, null, null);// ,
			// SchedulerUtils.createAbnormalContainerStatus(
			// container.getContainerId(),
			// SchedulerUtils.LOST_CONTAINER),
			// RMContainerEventType.KILL
		}

		// Remove the node
		this.nodes.remove(nodeInfo.getNodeID());
		// Update cluster metrics
		subtractFrom(clusterResource, node.getRMNode().getTotalCapability());
	}

	private synchronized void nodeUpdate(RMNode rmNode) {
		SchedulerNode node = getNode(rmNode.getNodeID());
		/**
		 * Get and clear the list of containerUpdates accumulated across NM
		 * heartbeats.
		 * 
		 * @return containerUpdates accumulated across NM heartbeats.
		 */
		List<UpdatedContainerInfo> containerInfoList = rmNode
				.pullContainerUpdates();
		List<ContainerStatus> newlyLaunchedContainers = new ArrayList<ContainerStatus>();
		List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
		for (UpdatedContainerInfo containerInfo : containerInfoList) {
			newlyLaunchedContainers.addAll(containerInfo
					.getNewlyLaunchedContainers());
			completedContainers.addAll(containerInfo.getCompletedContainers());
		}
		// Processing the newly launched containers
		for (ContainerStatus launchedContainer : newlyLaunchedContainers) {
			containerLaunchedOnNode(launchedContainer.getContainerId(), node);
		}

		// Process completed containers
		for (ContainerStatus completedContainer : completedContainers) {
			ContainerId containerId = completedContainer.getContainerId();
			LOG.debug("Container FINISHED: " + containerId);
			/** 更新相关数据结构，通知节点释放Container **/
			containerCompleted(getRMContainer(containerId), completedContainer,
					RMContainerEventType.FINISHED);
		}

		/** 判断node.getAvailableResource()是否大于minimumAllocation **/
		if (Resources.greaterThanOrEqual(resourceCalculator, clusterResource,
				node.getAvailableResource(), minimumAllocation)) {
			LOG.debug("Node heartbeat " + rmNode.getNodeID()
					+ " available resource = " + node.getAvailableResource());
			/**
			 * 资源分配算法的执行函数,新分配的资源加入到相应application
			 * 的相应数据结构newlyAllocatedContainers中
			 **/
			assignContainers(node);

			LOG.debug("Node after allocation " + rmNode.getNodeID()
					+ " resource = " + node.getAvailableResource());
		}
		// metrics.setAvailableResourcesToQueue(subtract(clusterResource,
		// usedResource));
	}

	private void containerLaunchedOnNode(ContainerId containerId,
			SchedulerNode node) {
		// Get the application for the finished container
		ApplicationAttemptId applicationAttemptId = containerId
				.getApplicationAttemptId();
		SchedulerApp application = getApplication(applicationAttemptId);
		if (application == null) {
			LOG.info("Unknown application: " + applicationAttemptId
					+ " launched container " + containerId + " on node: "
					+ node);
			// Some unknown container sneaked into the system. Kill it.
			// this.rmContext.getDispatcher().getEventHandler()
			// .handle(new RMNodeCleanContainerEvent(node.getNodeID(),
			// containerId));

			return;
		}
		/**
		 * Inform the container
		 */
		// application.containerLaunchedOnNode(containerId, node.getNodeID());
	}

	/**
	 * 资源分配算法的执行函数 获取可分配的资源，将资源封装成container，然后通知application和node更新相应记录 Heart of
	 * the scheduler...
	 * 
	 * @param node
	 *            node on which resources are available to be allocated
	 */
	private void assignContainers(SchedulerNode node) {

		LOG.debug("assignContainers:" + " node="
				+ node.getRMNode().getNodeAddress() + " #applications="
				+ applications.size());

		// Try to assign containers to applications in fifo order
		for (Map.Entry<ApplicationAttemptId, SchedulerApp> e : applications
				.entrySet()) {
			SchedulerApp application = e.getValue();
			LOG.debug("pre-assignContainers");
			/**
			 * 日志记录所有的请求
			 */
			application.showRequests();
			synchronized (application) {
				// Check if this resource is on the blacklist
				// if (FiCaSchedulerUtils.isBlacklisted(application, node, LOG))
				// {
				// continue;
				// }

				for (Priority priority : application.getPriorities()) {
					int maxContainers = getMaxAllocatableContainers(
							application, priority, node, NodeType.OFF_SWITCH);
					// Ensure the application needs containers of this priority
					if (maxContainers > 0) {
						/** 最后调用执行assignContainer（） **/
						int assignedContainers = assignContainersOnNode(node,
								application, priority);
						// Do not assign out of order w.r.t priorities
						if (assignedContainers == 0) {
							break;
						}
					}
				}
			}

			LOG.debug("post-assignContainers");
			application.showRequests();

			// Done
			if (Resources.lessThan(resourceCalculator, clusterResource,
					node.getAvailableResource(), minimumAllocation)) {
				break;
			}
		}

		// Update the applications' headroom to correctly take into
		// account the containers assigned in this update.
		for (SchedulerApp application : applications.values()) {
			application.setHeadroom(Resources.subtract(clusterResource,
					usedResource));
		}
	}

	private int getMaxAllocatableContainers(SchedulerApp application,
			Priority priority, SchedulerNode node, NodeType type) {
		ResourceRequest offSwitchRequest = application.getResourceRequest(
				priority, ResourceRequest.ANY);
		int maxContainers = offSwitchRequest.getNumContainers();

		if (type == NodeType.OFF_SWITCH) {
			return maxContainers;
		}

		if (type == NodeType.RACK_LOCAL) {
			ResourceRequest rackLocalRequest = application.getResourceRequest(
					priority, node.getRMNode().getRackName());
			if (rackLocalRequest == null) {
				return maxContainers;
			}

			maxContainers = Math.min(maxContainers,
					rackLocalRequest.getNumContainers());
		}

		if (type == NodeType.NODE_LOCAL) {
			ResourceRequest nodeLocalRequest = application.getResourceRequest(
					priority, node.getRMNode().getNodeAddress());
			if (nodeLocalRequest != null) {
				maxContainers = Math.min(maxContainers,
						nodeLocalRequest.getNumContainers());
			}
		}

		return maxContainers;
	}

	private int assignContainersOnNode(SchedulerNode node,
			SchedulerApp application, Priority priority) {
		// Data-local
		int nodeLocalContainers = assignNodeLocalContainers(node, application,
				priority);

		// Rack-local
		int rackLocalContainers = assignRackLocalContainers(node, application,
				priority);

		// Off-switch
		int offSwitchContainers = assignOffSwitchContainers(node, application,
				priority);

		LOG.debug("assignContainersOnNode:"
				+ " node="
				+ node.getRMNode().getNodeAddress()
				+ " application="
				+ application.getApplicationId().getId()
				+ " priority="
				+ priority.getPriority()
				+ " #assigned="
				+ (nodeLocalContainers + rackLocalContainers + offSwitchContainers));

		return (nodeLocalContainers + rackLocalContainers + offSwitchContainers);
	}

	private int assignNodeLocalContainers(SchedulerNode node,
			SchedulerApp application, Priority priority) {
		int assignedContainers = 0;
		ResourceRequest request = application.getResourceRequest(priority,
				node.getNodeName());
		if (request != null) {
			// Don't allocate on this node if we don't need containers on this
			// rack
			ResourceRequest rackRequest = application.getResourceRequest(
					priority, node.getRMNode().getRackName());
			if (rackRequest == null || rackRequest.getNumContainers() <= 0) {
				return 0;
			}

			int assignableContainers = Math.min(
					getMaxAllocatableContainers(application, priority, node,
							NodeType.NODE_LOCAL), request.getNumContainers());
			assignedContainers = assignContainer(node, application, priority,
					assignableContainers, request, NodeType.NODE_LOCAL);
		}
		return assignedContainers;
	}

	private int assignRackLocalContainers(SchedulerNode node,
			SchedulerApp application, Priority priority) {
		int assignedContainers = 0;
		ResourceRequest request = application.getResourceRequest(priority, node
				.getRMNode().getRackName());
		if (request != null) {
			// Don't allocate on this rack if the application doens't need
			// containers
			ResourceRequest offSwitchRequest = application.getResourceRequest(
					priority, ResourceRequest.ANY);
			if (offSwitchRequest.getNumContainers() <= 0) {
				return 0;
			}

			int assignableContainers = Math.min(
					getMaxAllocatableContainers(application, priority, node,
							NodeType.RACK_LOCAL), request.getNumContainers());
			assignedContainers = assignContainer(node, application, priority,
					assignableContainers, request, NodeType.RACK_LOCAL);
		}
		return assignedContainers;
	}

	private int assignOffSwitchContainers(SchedulerNode node,
			SchedulerApp application, Priority priority) {
		int assignedContainers = 0;
		ResourceRequest request = application.getResourceRequest(priority,
				ResourceRequest.ANY);
		if (request != null) {
			assignedContainers = assignContainer(node, application, priority,
					request.getNumContainers(), request, NodeType.OFF_SWITCH);
		}
		return assignedContainers;
	}

	private int assignContainer(SchedulerNode node, SchedulerApp application,
			Priority priority, int assignableContainers,
			ResourceRequest request, NodeType type) {
		LOG.debug("assignContainers:" + " node="
				+ node.getRMNode().getNodeAddress() + " application="
				+ application.getApplicationId().getId() + " priority="
				+ priority.getPriority() + " assignableContainers="
				+ assignableContainers + " request=" + request + " type="
				+ type);
		Resource capability = request.getCapability();

		int availableContainers = node.getAvailableResource().getMemory()
				/ capability.getMemory(); // TODO: A buggy
		// application
		// with this
		// zero would
		// crash the
		// scheduler.
		int assignedContainers = Math.min(assignableContainers,
				availableContainers);

		if (assignedContainers > 0) {
			for (int i = 0; i < assignedContainers; ++i) {

				NodeId nodeId = node.getRMNode().getNodeID();

				ContainerId containerId = new ContainerId(
						application.getApplicationAttemptId(),
						application.getNewContainerId());
				// Token containerToken = null;
				//
				// containerToken =
				// this.rmContext.getContainerTokenSecretManager()
				// .createContainerToken(containerId, nodeId,
				// application.getUser(),
				// capability);
				// if (containerToken == null) {
				// return i; // Try again later.
				// }
				/** 将资源封装到一个新的Container中 **/
				// Create the container
				Container container = new Container(containerId, nodeId, node
						.getRMNode().getHttpAddress(), capability, priority);

				// Allocate!

				// Inform the application
				RMContainer rmContainer = application.allocate(type, node,
						priority, request, container);

				// Inform the node
				node.allocateContainer(application.getApplicationId(),
						rmContainer);

				// Update usage for this container
				addTo(usedResource, capability);
			}

		}

		return assignedContainers;
	}

	SchedulerApp getApplication(ApplicationAttemptId applicationAttemptId) {
		return applications.get(applicationAttemptId);
	}

	private SchedulerNode getNode(NodeId nodeId) {
		return nodes.get(nodeId);
	}

	private Resource subtractFrom(Resource lhs, Resource rhs) {
		lhs.setMemory(lhs.getMemory() - rhs.getMemory());
		lhs.setVirtualCores(lhs.getVirtualCores() - rhs.getVirtualCores());
		return lhs;
	}

	private Resource addTo(Resource lhs, Resource rhs) {
		lhs.setMemory(lhs.getMemory() + rhs.getMemory());
		lhs.setVirtualCores(lhs.getVirtualCores() + rhs.getVirtualCores());
		return lhs;
	}

	/**
	 * 每个application的containerId是单独生成的
	 * 
	 * @param containerId
	 * @return
	 */
	private RMContainer getRMContainer(ContainerId containerId) {
		SchedulerApp application = getApplication(containerId
				.getApplicationAttemptId());
		return (application == null) ? null : application
				.getRMContainer(containerId);
	}

	private static Resource createResource(int memory) {
		return createResource(memory, (memory > 0) ? 1 : 0);
	}

	private static Resource createResource(int memory, int vCores) {
		return new Resource(memory, vCores);
	}

	/**
	 * Utility method to normalize a list of resource requests, by insuring that
	 * the memory for each request is a multiple of minMemory and is not zero.
	 */
	public static void normalizeRequests(List<ResourceRequest> asks,
			ResourceCalculator resourceCalculator, Resource clusterResource,
			Resource minimumResource, Resource maximumResource) {
		for (ResourceRequest ask : asks) {
			normalizeRequest(ask, resourceCalculator, clusterResource,
					minimumResource, maximumResource, minimumResource);
		}
	}

	/**
	 * Utility method to normalize a resource request, by insuring that the
	 * requested memory is a multiple of minMemory and is not zero.
	 */
	public static void normalizeRequest(ResourceRequest ask,
			ResourceCalculator resourceCalculator, Resource clusterResource,
			Resource minimumResource, Resource maximumResource,
			Resource incrementResource) {
		Resource normalized = Resources.normalize(resourceCalculator,
				ask.getCapability(), minimumResource, maximumResource,
				incrementResource);
		ask.setCapability(normalized);
	}

}
