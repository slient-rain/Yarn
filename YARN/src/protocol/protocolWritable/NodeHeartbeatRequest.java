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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import client.test.ClientMain;
import resourceManager.scheduler.ApplicationId;
import resourceManager.scheduler.ContainerStatus;
import resourceManager.scheduler.NodeId;
import rpc.io.UTF8;
import rpc.io.Writable;



public class NodeHeartbeatRequest implements Writable {
	private static final Logger LOG = LoggerFactory.getLogger(ClientMain.class);

	private NodeId nodeId = null;
	private List<ContainerStatus> containers = null;//ContainerStatus
	//	  private NodeHealthStatus nodeHealthStatus = null;
	private List<ApplicationId> keepAliveApplications = null;
	//  private MasterKey lastKnownContainerTokenMasterKey = null;
	//  private MasterKey lastKnownNMTokenMasterKey = null;
	public NodeHeartbeatRequest() {
		super();
		nodeId=new NodeId();
		containers=new ArrayList<ContainerStatus>();
		keepAliveApplications=new ArrayList<ApplicationId>();
	}
	public NodeHeartbeatRequest(NodeId nodeId, List<ContainerStatus> containers,
			List<ApplicationId> keepAliveApplications) {
		super();
		this.nodeId = nodeId;
		this.containers = containers;
		this.keepAliveApplications = keepAliveApplications;
	}
	public NodeId getNodeId() {
		return nodeId;
	}
	public void setNodeId(NodeId nodeId) {
		this.nodeId = nodeId;
	}
	public List<ContainerStatus> getContainers() {
		return containers;
	}
	public void setContainers(List<ContainerStatus> containers) {
		this.containers = containers;
	}
	public List<ApplicationId> getKeepAliveApplications() {
		return keepAliveApplications;
	}
	public void setKeepAliveApplications(List<ApplicationId> keepAliveApplications) {
		this.keepAliveApplications = keepAliveApplications;
	}
	@Override
	public String toString() {
		return "NodeHeartbeatRequest [nodeId=" + nodeId
				+ ", keepAliveApplications=" + keepAliveApplications + "]";
	}
	@Override
	public void write(DataOutput out) throws IOException {
		nodeId.write(out);
		out.writeInt(containers.size());
		for (ContainerStatus container : containers) {
			container.write(out);
		}
		out.writeInt(keepAliveApplications.size());
		for (ApplicationId app : keepAliveApplications) {
			app.write(out);
		}
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		nodeId.readFields(in);
		int n=in.readInt();
		LOG.debug("NodeHeartbeatRequest read container status , size: "+n);
		containers.clear();
		for(int i=0; i<n ;i++){
			ContainerStatus temp=new ContainerStatus();
			temp.readFields(in);
			containers.add(temp);
		}
		n=in.readInt();
		for(int i=0; i<n ;i++){
			ApplicationId temp=new ApplicationId();
			temp.readFields(in);
			keepAliveApplications.add(temp);
		}
	}
	//	public enum ContainerStatus{
	//		NEW,RUNNING,COMPLETE
	//	}



}  
