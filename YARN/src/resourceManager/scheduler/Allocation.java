package resourceManager.scheduler;

import java.util.List;
import java.util.Set;

public class Allocation {
//	private final RecordFactory recordFactory =
//			RecordFactoryProvider.getRecordFactory(null);

	final List<Container> containers;
	final Resource resourceLimit;
	final Set<ContainerId> strictContainers;
	final Set<ContainerId> fungibleContainers;
	final List<ResourceRequest> fungibleResources;

	public Allocation(List<Container> containers, Resource resourceLimit) {
		this(containers, resourceLimit, null, null, null);
	}

	public Allocation(List<Container> containers, Resource resourceLimit,
			Set<ContainerId> strictContainers) {
		this(containers, resourceLimit, strictContainers, null, null);
	}

	public Allocation(List<Container> containers, Resource resourceLimit,
			Set<ContainerId> strictContainers, Set<ContainerId> fungibleContainers,
			List<ResourceRequest> fungibleResources) {
		this.containers = containers;
		this.resourceLimit = resourceLimit;
		this.strictContainers = strictContainers;
		this.fungibleContainers = fungibleContainers;
		this.fungibleResources = fungibleResources;
	}

	public List<Container> getContainers() {
		return containers;
	}

	public Resource getResourceLimit() {
		return resourceLimit;
	}

	public Set<ContainerId> getStrictContainerPreemptions() {
		return strictContainers;
	}

	public Set<ContainerId> getContainerPreemptions() {
		return fungibleContainers;
	}

	public List<ResourceRequest> getResourcePreemptions() {
		return fungibleResources;
	}
}
