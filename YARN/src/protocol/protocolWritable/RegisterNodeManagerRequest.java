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

import resourceManager.scheduler.NodeId;
import resourceManager.scheduler.Resource;
import rpc.io.Writable;


public class RegisterNodeManagerRequest implements Writable {


	private Resource resource = null;
	private NodeId nodeId = null;

	public RegisterNodeManagerRequest() {
		resource=new Resource();
		nodeId=new NodeId();
	}

	public RegisterNodeManagerRequest(Resource resource,NodeId nodeId) {
		this.resource=resource;
		this.nodeId=nodeId;
	}

	public Resource getResource() {
		return resource;
	}

	public void setResource(Resource resource) {
		this.resource = resource;
	}

	public NodeId getNodeId() {
		return nodeId;
	}

	public void setNodeId(NodeId nodeId) {
		this.nodeId = nodeId;
	}


	public String getHost() {
		return nodeId.getHost();
	}



	public int getPort() {
		return nodeId.getPort();
	}


	@Override
	public String toString() {
		return "RegisterNodeManagerRequest [resource=" + resource + ", nodeId="
				+ nodeId + "]";
	}



	@Override
	public void write(DataOutput out) throws IOException {
		resource.write(out);
		nodeId.write(out);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		resource.readFields(in);
		nodeId.readFields(in);

	}




}  
