package resourceManager.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.org.apache.bcel.internal.generic.NEW;


/**
 * Represents an Application from the viewpoint of the scheduler.
 * Each running Application in the RM corresponds to one instance
 * of this class.
 */
@SuppressWarnings("unchecked")

public class SchedulerApp {

	@Override
	public String toString() {
		return "SchedulerApp [appSchedulingInfo=" + appSchedulingInfo
				+ ", currentConsumption=" + currentConsumption
				+ ", resourceLimit=" + resourceLimit + ", liveContainers="
				+ liveContainers + ", newlyAllocatedContainers="
				+ newlyAllocatedContainers + ", isStopped=" + isStopped + "]";
	}

	private static final Logger LOG = LoggerFactory.getLogger(SchedulerApp.class);
	private final AppSchedulingInfo appSchedulingInfo;
	/**
	 * 当前总消耗资源的统计信息
	 */
	private final Resource currentConsumption = new Resource();
	/**
	 * 整个集群的所有剩余资源空间
	 */
	private Resource resourceLimit = new Resource();
	private Map<ContainerId, RMContainer> liveContainers = new HashMap<ContainerId, RMContainer>();
	private List<RMContainer> newlyAllocatedContainers = new ArrayList<RMContainer>();
	private boolean isStopped = false;

	public SchedulerApp(ApplicationAttemptId applicationAttemptId
			) {
		this.appSchedulingInfo = 
				new AppSchedulingInfo(applicationAttemptId);
	}

	public synchronized void updateResourceRequests(
			List<ResourceRequest> requests) {
		if (!isStopped) {
			this.appSchedulingInfo.updateResourceRequests(requests);
		}
	}

	synchronized public RMContainer allocate(NodeType type, SchedulerNode node,
			Priority priority, ResourceRequest request, 
			Container container) {

		if (isStopped) {
			return null;
		}

		// Required sanity check - AM can call 'allocate' to update resource 
		// request without locking the scheduler, hence we need to check
		if (getTotalRequiredResources(priority) <= 0) {
			return null;
		}

		// Create RMContainer
		RMContainer rmContainer = new RMContainerImpl(container, this
				.getApplicationAttemptId(), node.getNodeID());
		//		 , this.rmContext.getDispatcher().getEventHandler(),
		//		 this.rmContext.getContainerAllocationExpirer()

		// Add it to allContainers list.
		newlyAllocatedContainers.add(rmContainer);
		liveContainers.put(container.getId(), rmContainer);    

		// Update consumption and track allocations
		appSchedulingInfo.allocate(type, node, priority, request, container);
		Resources.addTo(currentConsumption, container.getResource());

		// Inform the container
		rmContainer.handle(
				new RMContainerEvent(container.getId(), RMContainerEventType.START));

		if (LOG.isDebugEnabled()) {
			LOG.debug("assign: allocate: applicationAttemptId=" 
					+ container.getId().getApplicationAttemptId() 
					+ " container=" + container.getId() + " host="
					+ container.getNodeId().getHost() + " type=" + type);
		}
		//		 RMAuditLogger.logSuccess(getUser(), 
		//				 AuditConstants.ALLOC_CONTAINER, "SchedulerApp", 
		//				 getApplicationId(), container.getId());

		return rmContainer;
	}

	synchronized public List<Container> pullNewlyAllocatedContainers() {
		List<Container> returnContainerList = new ArrayList<Container>(
				newlyAllocatedContainers.size());
		for (RMContainer rmContainer : newlyAllocatedContainers) {
			rmContainer.handle(new RMContainerEvent(rmContainer.getContainerId(),
					RMContainerEventType.ACQUIRED));
			returnContainerList.add(rmContainer.getContainer());
		}
		newlyAllocatedContainers.clear();
		return returnContainerList;
	}

	synchronized public boolean containerCompleted(RMContainer rmContainer,
			ContainerStatus containerStatus, RMContainerEventType event) {

		// Remove from the list of containers
		if (null == liveContainers.remove(rmContainer.getContainerId())) {
			return false;
		}

		Container container = rmContainer.getContainer();
		ContainerId containerId = container.getId();

		// Inform the container ？？？？？？？？？？？？？先放到scheduler中去实现了
		//		 rmContainer.handle(
		//				 new RMContainerFinishedEvent(
		//						 containerId,
		//						 containerStatus, 
		//						 event)//FinishedTransition
		//				 );
		LOG.info("Completed container: " + rmContainer.getContainerId() + 
				" in state: " + rmContainer.getState() + " event:" + event);

		// containersToPreempt.remove(rmContainer.getContainerId());

		//		 RMAuditLogger.logSuccess(getUser(), AuditConstants.RELEASE_CONTAINER, "SchedulerApp", 
		//				 getApplicationId(), containerId);

		// Update usage metrics 
		Resource containerResource = rmContainer.getContainer().getResource();
		//queue.getMetrics().releaseResources(getUser(), 1, containerResource);
		subtractFrom(currentConsumption, containerResource);

		return true;
	}

	
	
	
	public ApplicationId getApplicationId() {
		return this.appSchedulingInfo.getApplicationId();
	}

	public ApplicationAttemptId getApplicationAttemptId() {
		return this.appSchedulingInfo.getApplicationAttemptId();
	}

	public Map<String, ResourceRequest> getResourceRequests(Priority priority) {
		return this.appSchedulingInfo.getResourceRequests(priority);
	}

	public int getNewContainerId() {
		return this.appSchedulingInfo.getNewContainerId();
	}

	public Collection<Priority> getPriorities() {
		return this.appSchedulingInfo.getPriorities();
	}

	public ResourceRequest getResourceRequest(Priority priority, String resourceName) {
		return this.appSchedulingInfo.getResourceRequest(priority, resourceName);
	}

	public synchronized int getTotalRequiredResources(Priority priority) {
		return getResourceRequest(priority, ResourceRequest.ANY).getNumContainers();
	}

	public Resource getResource(Priority priority) {
		return this.appSchedulingInfo.getResource(priority);
	}

	/**
	 * Get available headroom in terms of resources for the application's user.
	 * 整个集群的所有剩余资源空间
	 * @return available resource headroom
	 */
	public synchronized Resource getHeadroom() {
		// Corner case to deal with applications being slightly over-limit
		if (resourceLimit.getMemory() < 0) {
			resourceLimit.setMemory(0);
		}

		return resourceLimit;
	}

	public Resource getTotalPendingRequests() {
		Resource ret = Resource.newInstance(0, 0);
		for (ResourceRequest rr : appSchedulingInfo.getAllResourceRequests()) {
			// to avoid double counting we count only "ANY" resource requests
			if (ResourceRequest.isAnyLocation(rr.getResourceName())){
				Resources.addTo(ret,
						Resources.multiply(rr.getCapability(), rr.getNumContainers()));
			}
		}
		return ret;
	}

	/**
	 * Get the list of live containers
	 * @return All of the live containers
	 */

	public synchronized Collection<RMContainer> getLiveContainers() {
		return new ArrayList<RMContainer>(liveContainers.values());
	}

	public Resource getCurrentConsumption() {
		return this.currentConsumption;
	}


	public synchronized RMContainer getRMContainer(ContainerId id) {
		return liveContainers.get(id);
	}

	/**
	 *  整个集群的所有剩余资源空间
	 * @param globalLimit
	 */

	public synchronized void setHeadroom(Resource globalLimit) {
		this.resourceLimit = globalLimit; 
	}

	/**
	 * 日志记录所有的请求
	 */
	synchronized public void showRequests() {
		if (LOG.isDebugEnabled()) {
			for (Priority priority : getPriorities()) {
				Map<String, ResourceRequest> requests = getResourceRequests(priority);
				if (requests != null) {
					LOG.debug("showRequests:" + " application=" + getApplicationId() + 
							" headRoom=" + getHeadroom() + 
							" currentConsumption=" + currentConsumption.getMemory());
					for (ResourceRequest request : requests.values()) {
						LOG.debug("showRequests:" + " application=" + getApplicationId()
								+ " request=" + request.toString());
					}
				}
			}
		}
	}

	/**
	 * Is this application pending?
	 * @return true if it is else false.
	 */

	public boolean isPending() {
		return this.appSchedulingInfo.isPending();
	}

	public synchronized boolean isStopped() {
		return this.isStopped;
	}

	public synchronized void stop(RMAppAttemptState rmAppAttemptFinalState) {
		// Cleanup all scheduling information
		this.isStopped = true;
		this.appSchedulingInfo.stop(rmAppAttemptFinalState);
	}

	private Resource subtractFrom(Resource lhs, Resource rhs) {
		lhs.setMemory(lhs.getMemory() - rhs.getMemory());
		lhs.setVirtualCores(lhs.getVirtualCores() - rhs.getVirtualCores());
		return lhs;
	}











	//		final Map<Priority, Map<NodeId, RMContainer>> reservedContainers = 
	//		new HashMap<Priority, Map<NodeId, RMContainer>>();	

	//private final Set<ContainerId> containersToPreempt =
	//		new HashSet<ContainerId>();

	/**
	 * Count how many times the application has been given an opportunity
	 * to schedule a task at each priority. Each time the scheduler
	 * asks the application for a task at this priority, it is incremented,
	 * and each time the application successfully schedules a task, it
	 * is reset to 0.
	 */
	//Multiset<Priority> schedulingOpportunities = HashMultiset.create();

	//Multiset<Priority> reReservations = HashMultiset.create();

	//Resource currentReservation = recordFactory
	//		.newRecordInstance(Resource.class);
	//
	//private final RMContext rmContext;




	//		public String getUser() {
	//		return this.appSchedulingInfo.getUser();
	//	}

	//	 public synchronized void addPreemptContainer(ContainerId cont){
	//	 // ignore already completed containers
	//	 if (liveContainers.containsKey(cont)) {
	//		 containersToPreempt.add(cont);
	//	 }
	// }

	/**
	 * This method produces an Allocation that includes the current view
	 * of the resources that will be allocated to and preempted from this
	 * application.
	 *
	 * @param rc
	 * @param clusterResource
	 * @param minimumAllocation
	 * @return an allocation
	 */
	// public synchronized Allocation getAllocation(ResourceCalculator rc,
	//		 Resource clusterResource, Resource minimumAllocation) {
	//
	//	 Set<ContainerId> currentContPreemption = Collections.unmodifiableSet(
	//			 new HashSet<ContainerId>(containersToPreempt));
	//	 containersToPreempt.clear();
	//	 Resource tot = Resource.newInstance(0, 0);
	//	 for(ContainerId c : currentContPreemption){
	//		 Resources.addTo(tot,
	//				 liveContainers.get(c).getContainer().getResource());
	//	 }
	//	 int numCont = (int) Math.ceil(
	//			 Resources.divide(rc, clusterResource, tot, minimumAllocation));
	//	 ResourceRequest rr = ResourceRequest.newInstance(
	//			 Priority.UNDEFINED, ResourceRequest.ANY,
	//			 minimumAllocation, numCont);
	//	 return new Allocation(pullNewlyAllocatedContainers(), getHeadroom(),
	//			 null, currentContPreemption,
	//			 Collections.singletonList(rr));
	// }


	//	 public Queue getQueue() {
	//		 return queue;
	//	 }

	//	 synchronized public void resetSchedulingOpportunities(Priority priority) {
	//	 this.schedulingOpportunities.setCount(priority, 0);
	// }
	//
	// synchronized public void addSchedulingOpportunity(Priority priority) {
	//	 this.schedulingOpportunities.setCount(priority,
	//			 schedulingOpportunities.count(priority) + 1);
	// }
	//
	// synchronized public void subtractSchedulingOpportunity(Priority priority) {
	//	 int count = schedulingOpportunities.count(priority) - 1;
	//	 this.schedulingOpportunities.setCount(priority, Math.max(count,  0));
	// }

	/**
	 * @param priority Target priority
	 * @return the number of times the application has been given an opportunity
	 * to schedule a task at the given priority since the last time it
	 * successfully did so.
	 */
	// synchronized public int getSchedulingOpportunities(Priority priority) {
	//	 return this.schedulingOpportunities.count(priority);
	// }
	//
	// synchronized void resetReReservations(Priority priority) {
	//	 this.reReservations.setCount(priority, 0);
	// }
	//
	// synchronized void addReReservation(Priority priority) {
	//	 this.reReservations.add(priority);
	// }
	//
	// synchronized public int getReReservations(Priority priority) {
	//	 return this.reReservations.count(priority);
	// }
	//
	// public synchronized int getNumReservedContainers(Priority priority) {
	//	 Map<NodeId, RMContainer> reservedContainers = 
	//			 this.reservedContainers.get(priority);
	//	 return (reservedContainers == null) ? 0 : reservedContainers.size();
	// }

	/**
	 * Get total current reservations.
	 * Used only by unit tests
	 * @return total current reservations
	 */

	// public synchronized Resource getCurrentReservation() {
	//	 return currentReservation;
	// }
	//
	// public synchronized RMContainer reserve(FiCaSchedulerNode node, Priority priority,
	//		 RMContainer rmContainer, Container container) {
	//	 // Create RMContainer if necessary
	//	 if (rmContainer == null) {
	//		 rmContainer = 
	//				 new RMContainerImpl(container, getApplicationAttemptId(), 
	//						 node.getNodeID(), rmContext.getDispatcher().getEventHandler(), 
	//						 rmContext.getContainerAllocationExpirer());
	//
	//		 Resources.addTo(currentReservation, container.getResource());
	//
	//		 // Reset the re-reservation count
	//		 resetReReservations(priority);
	//	 } else {
	//		 // Note down the re-reservation
	//		 addReReservation(priority);
	//	 }
	//	 rmContainer.handle(new RMContainerReservedEvent(container.getId(), 
	//			 container.getResource(), node.getNodeID(), priority));
	//
	//	 Map<NodeId, RMContainer> reservedContainers = 
	//			 this.reservedContainers.get(priority);
	//	 if (reservedContainers == null) {
	//		 reservedContainers = new HashMap<NodeId, RMContainer>();
	//		 this.reservedContainers.put(priority, reservedContainers);
	//	 }
	//	 reservedContainers.put(node.getNodeID(), rmContainer);
	//
	//	 LOG.info("Application " + getApplicationId() 
	//			 + " reserved container " + rmContainer
	//			 + " on node " + node + ", currently has " + reservedContainers.size()
	//			 + " at priority " + priority 
	//			 + "; currentReservation " + currentReservation.getMemory());
	//
	//	 return rmContainer;
	// }
	//
	// public synchronized boolean unreserve(FiCaSchedulerNode node, Priority priority) {
	//	 Map<NodeId, RMContainer> reservedContainers =
	//			 this.reservedContainers.get(priority);
	//
	//	 if (reservedContainers != null) {
	//		 RMContainer reservedContainer = reservedContainers.remove(node.getNodeID());
	//
	//		 // unreserve is now triggered in new scenarios (preemption)
	//		 // as a consequence reservedcontainer might be null, adding NP-checks
	//		 if (reservedContainer != null
	//				 && reservedContainer.getContainer() != null
	//				 && reservedContainer.getContainer().getResource() != null) {
	//
	//			 if (reservedContainers.isEmpty()) {
	//				 this.reservedContainers.remove(priority);
	//			 }
	//			 // Reset the re-reservation count
	//			 resetReReservations(priority);
	//
	//			 Resource resource = reservedContainer.getContainer().getResource();
	//			 Resources.subtractFrom(currentReservation, resource);
	//
	//			 LOG.info("Application " + getApplicationId() + " unreserved "
	//					 + " on node " + node + ", currently has " + reservedContainers.size()
	//					 + " at priority " + priority + "; currentReservation "
	//					 + currentReservation);
	//			 return true;
	//		 }
	//	 }
	//	 return false;
	// }
	//
	// /**
	//  * Has the application reserved the given <code>node</code> at the
	//  * given <code>priority</code>?
	//  * @param node node to be checked
	//  * @param priority priority of reserved container
	//  * @return true is reserved, false if not
	//  */
	// public synchronized boolean isReserved(FiCaSchedulerNode node, Priority priority) {
	//	 Map<NodeId, RMContainer> reservedContainers = 
	//			 this.reservedContainers.get(priority);
	//	 if (reservedContainers != null) {
	//		 return reservedContainers.containsKey(node.getNodeID());
	//	 }
	//	 return false;
	// }

	// public synchronized float getLocalityWaitFactor(
	//		 Priority priority, int clusterNodes) {
	//	 // Estimate: Required unique resources (i.e. hosts + racks)
	//	 int requiredResources = 
	//			 Math.max(this.getResourceRequests(priority).size() - 1, 0);
	//
	//	 // waitFactor can't be more than '1' 
	//	 // i.e. no point skipping more than clustersize opportunities
	//	 return Math.min(((float)requiredResources / clusterNodes), 1.0f);
	// }

	/**
	 * Get the list of reserved containers
	 * @return All of the reserved containers.
	 */

	// public synchronized List<RMContainer> getReservedContainers() {
	//	 List<RMContainer> reservedContainers = new ArrayList<RMContainer>();
	//	 for (Map.Entry<Priority, Map<NodeId, RMContainer>> e : 
	//		 this.reservedContainers.entrySet()) {
	//		 reservedContainers.addAll(e.getValue().values());
	//	 }
	//	 return reservedContainers;
	// }

	//		public synchronized void updateBlacklist(
	//		List<String> blacklistAdditions, List<String> blacklistRemovals) {
	//	if (!isStopped) {
	//		this.appSchedulingInfo.updateBlacklist(
	//				blacklistAdditions, blacklistRemovals);
	//	}
	//}

	//		public boolean isBlacklisted(String resourceName) {
	//			return this.appSchedulingInfo.isBlacklisted(resourceName);
	//		}

	//	 public synchronized void containerLaunchedOnNode(ContainerId containerId,
	//	 NodeId nodeId) {
	// // Inform the container
	// RMContainer rmContainer = 
	//		 getRMContainer(containerId);
	// if (rmContainer == null) {
	//	 // Some unknown container sneaked into the system. Kill it.
	//	 this.rmContext.getDispatcher().getEventHandler()
	//	 .handle(new RMNodeCleanContainerEvent(nodeId, containerId));
	//	 return;
	// }
	// 
	// rmContainer.handle(new RMContainerEvent(containerId,
	//		 RMContainerEventType.LAUNCHED));
	//}

	//	 public String getQueueName() {
	//		 return this.appSchedulingInfo.getQueueName();
	//	 }
}
