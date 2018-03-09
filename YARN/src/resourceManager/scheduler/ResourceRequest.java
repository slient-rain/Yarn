package resourceManager.scheduler;

import java.io.Serializable;

/**
 * <p><code>ResourceRequest</code> represents the request made by an
 * application to the <code>ResourceManager</code> to obtain various 
 * <code>Container</code> allocations.</p>
 * 
 * <p>It includes:
 *   <ul>
 *     <li>{@link Priority} of the request.</li>
 *     <li>
 *       The <em>name</em> of the machine or rack on which the allocation is 
 *       desired. A special value of <em>*</em> signifies that 
 *       <em>any</em> host/rack is acceptable to the application.
 *     </li>
 *     <li>{@link Resource} required for each request.</li>
 *     <li>
 *       Number of containers, of above specifications, which are required 
 *       by the application.
 *     </li>
 *     <li>
 *       A boolean <em>relaxLocality</em> flag, defaulting to <code>true</code>,
 *       which tells the <code>ResourceManager</code> if the application wants
 *       locality to be loose (i.e. allows fall-through to rack or <em>any</em>)
 *       or strict (i.e. specify hard constraint on resource allocation).
 *     </li>
 *   </ul>
 * </p>
 * 
 * @see Resource
 * @see ApplicationMasterProtocol#allocate(org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest)
 */


public class ResourceRequest implements Comparable<ResourceRequest> {

	@Override
	public String toString() {
		return "ResourceRequest [priority=" + priority + ", hostName="
				+ hostName + ", capability=" + capability + ", numContainers="
				+ numContainers + ", relaxLocality=" + relaxLocality + "]";
	}

	Priority priority;
	String hostName;
	Resource capability;
	int numContainers;
	boolean relaxLocality;
	
	
	public  ResourceRequest (Priority priority, String hostName,
			Resource capability, int numContainers) {
		this(priority, hostName, capability, numContainers, true);
	}

	
	public ResourceRequest(Priority priority, String hostName,
			Resource capability, int numContainers, boolean relaxLocality) {
		super();
		this.priority = priority;
		this.hostName = hostName;
		this.capability = capability;
		this.numContainers = numContainers;
		this.relaxLocality = relaxLocality;
	}







	public static class ResourceRequestComparator implements
	java.util.Comparator<ResourceRequest>, Serializable {

		private static final long serialVersionUID = 1L;

		@Override
		public int compare(ResourceRequest r1, ResourceRequest r2) {

			// Compare priority, host and capability
			int ret = r1.getPriority().compareTo(r2.getPriority());
			if (ret == 0) {
				String h1 = r1.getResourceName();
				String h2 = r2.getResourceName();
				ret = h1.compareTo(h2);
			}
			if (ret == 0) {
				ret = r1.getCapability().compareTo(r2.getCapability());
			}
			return ret;
		}
	}

	/**
	 * The constant string representing no locality.
	 * It should be used by all references that want to pass an arbitrary host
	 * name in.
	 */
	public static final String ANY = "*";

	/**
	 * Check whether the given <em>host/rack</em> string represents an arbitrary
	 * host name.
	 *
	 * @param hostName <em>host/rack</em> on which the allocation is desired
	 * @return whether the given <em>host/rack</em> string represents an arbitrary
	 * host name
	 */
	
	
	public static boolean isAnyLocation(String hostName) {
		return ANY.equals(hostName);
	}

	/**
	 * Get the <code>Priority</code> of the request.
	 * @return <code>Priority</code> of the request
	 */
	
	
	public  Priority getPriority(){
		return priority;
	};

	/**
	 * Set the <code>Priority</code> of the request
	 * @param priority <code>Priority</code> of the request
	 */
	
	
	public  void setPriority(Priority priority){
		this.priority=priority;
	}

	/**
	 * Get the resource (e.g. <em>host/rack</em>) on which the allocation 
	 * is desired.
	 * 
	 * A special value of <em>*</em> signifies that <em>any</em> resource 
	 * (host/rack) is acceptable.
	 * 
	 * @return resource (e.g. <em>host/rack</em>) on which the allocation 
	 *                  is desired
	 */
	
	
	public  String getResourceName(){
		return hostName;
	};;

	/**
	 * Set the resource name (e.g. <em>host/rack</em>) on which the allocation 
	 * is desired.
	 * 
	 * A special value of <em>*</em> signifies that <em>any</em> resource name
	 * (e.g. host/rack) is acceptable. 
	 * 
	 * @param resourceName (e.g. <em>host/rack</em>) on which the 
	 *                     allocation is desired
	 */
	
	
	public  void setResourceName(String resourceName){
		this.hostName=resourceName;
	}
	/**
	 * Get the <code>Resource</code> capability of the request.
	 * @return <code>Resource</code> capability of the request
	 */
	
	
	public  Resource getCapability(){
		return capability;
	};

	/**
	 * Set the <code>Resource</code> capability of the request
	 * @param capability <code>Resource</code> capability of the request
	 */
	
	
	public  void setCapability(Resource capability){
		this.capability=capability;
	}
	/**
	 * Get the number of containers required with the given specifications.
	 * @return number of containers required with the given specifications
	 */
	
	
	public  int getNumContainers(){
		return numContainers;
	};
	/**
	 * Set the number of containers required with the given specifications
	 * @param numContainers number of containers required with the given 
	 *                      specifications
	 */
	
	
	public  void setNumContainers(int numContainers){
		this.numContainers=numContainers;
	}
	/**
	 * Get whether locality relaxation is enabled with this
	 * <code>ResourceRequest</code>. Defaults to true.
	 * 
	 * @return whether locality relaxation is enabled with this
	 * <code>ResourceRequest</code>.
	 */
	
	
	public  boolean getRelaxLocality(){
		return relaxLocality;
	};
	/**
	 * <p>For a request at a network hierarchy level, set whether locality can be relaxed
	 * to that level and beyond.<p>
	 * 
	 * <p>If the flag is off on a rack-level <code>ResourceRequest</code>,
	 * containers at that request's priority will not be assigned to nodes on that
	 * request's rack unless requests specifically for those nodes have also been
	 * submitted.<p>
	 * 
	 * <p>If the flag is off on an {@link ResourceRequest#ANY}-level
	 * <code>ResourceRequest</code>, containers at that request's priority will
	 * only be assigned on racks for which specific requests have also been
	 * submitted.<p>
	 * 
	 * <p>For example, to request a container strictly on a specific node, the
	 * corresponding rack-level and any-level requests should have locality
	 * relaxation set to false.  Similarly, to request a container strictly on a
	 * specific rack, the corresponding any-level request should have locality
	 * relaxation set to false.<p>
	 * 
	 * @param relaxLocality whether locality relaxation is enabled with this
	 * <code>ResourceRequest</code>.
	 */
	
	
	public  void setRelaxLocality(boolean relaxLocality){
		this.relaxLocality=relaxLocality;
	}
	@Override
	public int hashCode() {
		final int prime = 2153;
		int result = 2459;
		Resource capability = getCapability();
		String hostName = getResourceName();
		Priority priority = getPriority();
		result =
				prime * result + ((capability == null) ? 0 : capability.hashCode());
		result = prime * result + ((hostName == null) ? 0 : hostName.hashCode());
		result = prime * result + getNumContainers();
		result = prime * result + ((priority == null) ? 0 : priority.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ResourceRequest other = (ResourceRequest) obj;
		Resource capability = getCapability();
		if (capability == null) {
			if (other.getCapability() != null)
				return false;
		} else if (!capability.equals(other.getCapability()))
			return false;
		String hostName = getResourceName();
		if (hostName == null) {
			if (other.getResourceName() != null)
				return false;
		} else if (!hostName.equals(other.getResourceName()))
			return false;
		if (getNumContainers() != other.getNumContainers())
			return false;
		Priority priority = getPriority();
		if (priority == null) {
			if (other.getPriority() != null)
				return false;
		} else if (!priority.equals(other.getPriority()))
			return false;
		return true;
	}

	@Override
	public int compareTo(ResourceRequest other) {
		int priorityComparison = this.getPriority().compareTo(other.getPriority());
		if (priorityComparison == 0) {
			int hostNameComparison =
					this.getResourceName().compareTo(other.getResourceName());
			if (hostNameComparison == 0) {
				int capabilityComparison =
						this.getCapability().compareTo(other.getCapability());
				if (capabilityComparison == 0) {
					int numContainersComparison =
							this.getNumContainers() - other.getNumContainers();
					if (numContainersComparison == 0) {
						return 0;
					} else {
						return numContainersComparison;
					}
				} else {
					return capabilityComparison;
				}
			} else {
				return hostNameComparison;
			}
		} else {
			return priorityComparison;
		}
	}
}