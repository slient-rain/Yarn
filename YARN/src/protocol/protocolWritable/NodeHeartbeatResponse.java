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

import resourceManager.scheduler.ApplicationId;
import resourceManager.scheduler.ContainerId;
import rpc.io.UTF8;
import rpc.io.Writable;
public class NodeHeartbeatResponse implements Writable {


	private List<ContainerId> containersToCleanup = null;
	private List<ApplicationId> applicationsToCleanup = null;
	String nodeAction;
	long nextHeartBeatInterval;
	public NodeHeartbeatResponse() {
		super();
		containersToCleanup=new ArrayList<ContainerId>();
		applicationsToCleanup=new ArrayList<ApplicationId>();
	}
	public NodeHeartbeatResponse(List<ContainerId> containersToCleanup,
			List<ApplicationId> applicationsToCleanup, String nodeAction,
			long nextHeartBeatInterval) {
		super();
		this.containersToCleanup = containersToCleanup;
		this.applicationsToCleanup = applicationsToCleanup;
		this.nodeAction = nodeAction;
		this.nextHeartBeatInterval = nextHeartBeatInterval;
	}
	public List<ContainerId> getContainersToCleanup() {
		return containersToCleanup;
	}
	public void setContainersToCleanup(List<ContainerId> containersToCleanup) {
		this.containersToCleanup = containersToCleanup;
	}
	public void addAllContainersToCleanup(List<ContainerId> containersToCleanup) {
		this.containersToCleanup = containersToCleanup;
	}
	public List<ApplicationId> getApplicationsToCleanup() {
		return applicationsToCleanup;
	}
	public void setApplicationsToCleanup(List<ApplicationId> applicationsToCleanup) {
		this.applicationsToCleanup = applicationsToCleanup;
	}
	public void addAllApplicationsToCleanup(List<ApplicationId> applicationsToCleanup) {
		this.applicationsToCleanup = applicationsToCleanup;
	}
	public String getNodeAction() {
		return nodeAction;
	}
	public void setNodeAction(String nodeAction) {
		this.nodeAction = nodeAction;
	}
	public long getNextHeartBeatInterval() {
		return nextHeartBeatInterval;
	}
	public void setNextHeartBeatInterval(long nextHeartBeatInterval) {
		this.nextHeartBeatInterval = nextHeartBeatInterval;
	}
	@Override
	public String toString() {
		return "NodeHeartbeatResponse [containersToCleanup=" + containersToCleanup
				+ ", applicationsToCleanup=" + applicationsToCleanup
				+ ", nodeAction=" + nodeAction + ", nextHeartBeatInterval="
				+ nextHeartBeatInterval + "]";
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(containersToCleanup.size());
		for (ContainerId container : containersToCleanup) {
			container.write(out);
		}
		out.writeInt(applicationsToCleanup.size());
		for (ApplicationId app : applicationsToCleanup) {
			app.write(out);
		}
		UTF8.writeString(out, nodeAction);
		out.writeLong(nextHeartBeatInterval);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		int n=in.readInt();
		containersToCleanup.clear();
		for (int i=0; i<n ;i++) {
			ContainerId container=new ContainerId();
			container.readFields(in);
			containersToCleanup.add(container);
		}
		n=in.readInt();
		applicationsToCleanup.clear();
		for (int i=0; i<n ;i++) {
			ApplicationId app=new ApplicationId();
			app.readFields(in);
			applicationsToCleanup.add(app);
		}
		nodeAction=UTF8.readString(in);
		nextHeartBeatInterval=in.readLong();

	}




}

