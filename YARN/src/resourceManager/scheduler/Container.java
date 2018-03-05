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

package resourceManager.scheduler;



/**
 * <p><code>Container</code> represents an allocated resource in the cluster.
 * </p>
 * 
 * <p>The <code>ResourceManager</code> is the sole authority to allocate any
 * <code>Container</code> to applications. The allocated <code>Container</code>
 * is always on a single node and has a unique {@link ContainerId}. It has
 * a specific amount of {@link Resource} allocated.</p>
 * 
 * <p>It includes details such as:
 *   <ul>
 *     <li>{@link ContainerId} for the container, which is globally unique.</li>
 *     <li>
 *       {@link NodeId} of the node on which it is allocated.
 *     </li>
 *     <li>HTTP uri of the node.</li>
 *     <li>{@link Resource} allocated to the container.</li>
 *     <li>{@link Priority} at which the container was allocated.</li>
 *   </ul>
 * </p>
 * 
 * <p>Typically, an <code>ApplicationMaster</code> receives the 
 * <code>Container</code> from the <code>ResourceManager</code> during
 * resource-negotiation and then talks to the <code>NodeManager</code> to 
 * start/stop containers.</p>
 * 
 * @see ApplicationMasterProtocol#allocate(org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest)
 * @see ContainerManagementProtocol#startContainers(org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest)
 * @see ContainerManagementProtocol#stopContainers(org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest)
 */

public  class Container implements Comparable<Container> {
	@Override
	public String toString() {
		return "Container [id=" + id + ", nodeId=" + nodeId
				+ ", nodeHttpAddress=" + nodeHttpAddress + ", resource="
				+ resource + ", priority=" + priority + "]";
	}
	ContainerId id;
	NodeId nodeId;
	String nodeHttpAddress;
	Resource resource;
	Priority priority;
	public Container(ContainerId id, NodeId nodeId, String nodeHttpAddress,
			Resource resource, Priority priority) {
		super();
		this.id = id;
		this.nodeId = nodeId;
		this.nodeHttpAddress = nodeHttpAddress;
		this.resource = resource;
		this.priority = priority;
	}
	public ContainerId getContainerId() {
		return id;
	}
	public ContainerId getId() {
		return id;
	}
	public void setId(ContainerId id) {
		this.id = id;
	}
	public NodeId getNodeId() {
		return nodeId;
	}
	public void setNodeId(NodeId nodeId) {
		this.nodeId = nodeId;
	}
	public String getNodeHttpAddress() {
		return nodeHttpAddress;
	}
	public void setNodeHttpAddress(String nodeHttpAddress) {
		this.nodeHttpAddress = nodeHttpAddress;
	}
	public Resource getResource() {
		return resource;
	}
	public void setResource(Resource resource) {
		this.resource = resource;
	}
	public Priority getPriority() {
		return priority;
	}
	public void setPriority(Priority priority) {
		this.priority = priority;
	}
	@Override
	public int compareTo(Container o) {
		// TODO Auto-generated method stub
		return 0;
	}

	
 
}
