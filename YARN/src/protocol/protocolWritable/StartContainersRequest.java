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
import java.util.ArrayList;
import java.util.List;

import com.sun.org.apache.bcel.internal.generic.NEW;

import rpc.io.UTF8;
import rpc.io.Writable;


/**
 * <p>
 * The request which contains a list of {@link StartContainerRequest} sent by
 * the <code>ApplicationMaster</code> to the <code>NodeManager</code> to
 * <em>start</em> containers.
 * </p>
 * 
 * <p>
 * In each {@link StartContainerRequest}, the <code>ApplicationMaster</code> has
 * to provide details such as allocated resource capability, security tokens (if
 * enabled), command to be executed to start the container, environment for the
 * process, necessary binaries/jar/shared-objects etc. via the
 * {@link ContainerLaunchContext}.
 * </p>
 * 
 * @see ContainerManagementProtocol#startContainers(StartContainersRequest)
 */

public class StartContainersRequest implements Writable{
	 List<StartContainerRequest> requests=new ArrayList<StartContainerRequest>();

	public StartContainersRequest() {
		super();
		// TODO Auto-generated constructor stub
	}

	public StartContainersRequest(List<StartContainerRequest> requests) {
		super();
		this.requests = requests;
	}

	public List<StartContainerRequest> getRequests() {
		return requests;
	}

	public void setRequests(List<StartContainerRequest> requests) {
		this.requests = requests;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		int n=requests.size();
		out.writeInt(n);
		for(int i=0; i<n; i++){
			requests.get(i).write(out);
		}
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int n=in.readInt();
		requests.clear();
		for(int i=0; i<n; i++){
			StartContainerRequest request=new StartContainerRequest();
			request.readFields(in);
			requests.add(request);
		}
		
	}

	@Override
	public String toString() {
		return "StartContainersRequest [requests=" + requests + "]";
	}


	 
  
}
