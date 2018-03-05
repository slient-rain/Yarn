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

package protocol.protocolWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



import protocol.protocolWritable.ApplicationSubmissionContext.LocalResource;


import resourceManager.scheduler.ApplicationId;
import resourceManager.scheduler.ContainerId;
import resourceManager.scheduler.Resource;
import rpc.io.UTF8;
import rpc.io.Writable;

/**
 * <p><code>ContainerLaunchContext</code> represents all of the information
 * needed by the <code>NodeManager</code> to launch a container.</p>
 * 
 * <p>It includes details such as:
 *   <ul>
 *     <li>{@link ContainerId} of the container.</li>
 *     <li>{@link Resource} allocated to the container.</li>
 *     <li>User to whom the container is allocated.</li>
 *     <li>Security tokens (if security is enabled).</li>
 *     <li>
 *       {@link LocalResource} necessary for running the container such
 *       as binaries, jar, shared-objects, side-files etc. 
 *     </li>
 *     <li>Optional, application-specific binary service data.</li>
 *     <li>Environment variables for the launched process.</li>
 *     <li>Command to launch the container.</li>
 *   </ul>
 * </p>
 * 
 * @see ContainerManagementProtocol#startContainers(org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest)
 */

public  class ContainerLaunchContext implements Writable{



	Map<String, LocalResource> localResources=new HashMap<String, ApplicationSubmissionContext.LocalResource>();
	Map<String, String> environment=new HashMap<String, String>();
	List<String> commands=new ArrayList<String>();
	Resource resource=new Resource();
	ContainerId containerId= new ContainerId();
	ApplicationId applicationId=new ApplicationId();
	String user;
	//      Map<String, ByteBuffer> serviceData;
	//      ByteBuffer tokens;
	//      Map<ApplicationAccessType, String> acls;

	public ContainerLaunchContext() {
		super();
		// TODO Auto-generated constructor stub
	}





	public String getUser() {
		return user;
	}





	public void setUser(String user) {
		this.user = user;
	}





	public ContainerLaunchContext(Map<String, LocalResource> localResources,
			Map<String, String> environment, List<String> commands,
			Resource resource, ContainerId containerId,
			ApplicationId applicationId, String user) {
		super();
		this.localResources = localResources;
		this.environment = environment;
		this.commands = commands;
		this.resource = resource;
		this.containerId = containerId;
		this.applicationId = applicationId;
		this.user = user;
	}





	@Override
	public String toString() {
		return "ContainerLaunchContext [localResources=" + localResources.toString()
				+ ", environment=" + environment + ", commands=" + commands
				+ ", resource=" + resource.toString() + ", containerId=" + containerId.toString()
				+ ", applicationId=" + applicationId.toString() + "]"+", user="+user;
	}


	public Map<String, LocalResource> getLocalResources() {
		return localResources;
	}


	public void setLocalResources(Map<String, LocalResource> localResources) {
		this.localResources = localResources;
	}


	public Map<String, String> getEnvironment() {
		return environment;
	}


	public void setEnvironment(Map<String, String> environment) {
		this.environment = environment;
	}


	public List<String> getCommands() {
		return commands;
	}


	public void setCommands(List<String> commands) {
		this.commands = commands;
	}


	public Resource getResource() {
		return resource;
	}


	public void setResource(Resource resource) {
		this.resource = resource;
	}


	public ContainerId getContainerId() {
		return containerId;
	}


	public void setContainerId(ContainerId containerId) {
		this.containerId = containerId;
	}


	public ApplicationId getApplicationId() {
		return applicationId;
	}


	public void setApplicationId(ApplicationId applicationId) {
		this.applicationId = applicationId;
	}


	@Override
	public void write(DataOutput out) throws IOException {
		int n=localResources.size();
		out.writeInt(n);
		for(Map.Entry<String, LocalResource> entry:localResources.entrySet()){
			UTF8.writeString(out, entry.getKey());
			entry.getValue().write(out);
		}
		n=environment.size();
		out.writeInt(n);
		for(Map.Entry<String, String> entry:environment.entrySet()){
			UTF8.writeString(out, entry.getKey());
			UTF8.writeString(out, entry.getValue());
		}
		n=commands.size();
		out.writeInt(n);
		for(int i=0; i<n; i++){
			UTF8.writeString(out, commands.get(i));
		}

		containerId.write(out);
		applicationId.write(out);
		resource.write(out);
		
		UTF8.writeString(out, user);

	}



	@Override
	public void readFields(DataInput in) throws IOException {
		int n=in.readInt();
		localResources.clear();
		for(int i=0; i<n; i++){
			LocalResource value=(new ApplicationSubmissionContext()).new LocalResource();
			String key=UTF8.readString(in);
			value.readFields(in);
			localResources.put(key, value);
		}
		n=in.readInt();
		environment.clear();
		for(int i=0; i<n; i++){
			String key=UTF8.readString(in);
			String value=UTF8.readString(in);
			environment.put(key, value);
		}
		n=in.readInt();
		commands.clear();
		for(int i=0; i<n; i++){
			String key=UTF8.readString(in);
			commands.add(key);
		}

		containerId.readFields(in);
		applicationId.readFields(in);
		resource.readFields(in);
		
		user=UTF8.readString(in);
	}
}
