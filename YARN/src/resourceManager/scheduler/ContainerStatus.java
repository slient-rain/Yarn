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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import client.test.ClientMain;
import rpc.io.UTF8;
import rpc.io.Writable;


/**
 * <p><code>ContainerStatus</code> represents the current status of a 
 * <code>Container</code>.</p>
 * 
 * <p>It provides details such as:
 *   <ul>
 *     <li><code>ContainerId</code> of the container.</li>
 *     <li><code>ContainerState</code> of the container.</li>
 *     <li><em>Exit status</em> of a completed container.</li>
 *     <li><em>Diagnostic</em> message for a failed container.</li>
 *   </ul>
 * </p>
 */


public class ContainerStatus  implements Writable{
	ContainerId containerId;
	ContainerState state;
	private static final Logger LOG = LoggerFactory.getLogger(ClientMain.class);
	
	/**
	 * <p>Get the <em>exit status</em> for the container.</p>
	 *  
	 * <p>Note: This is valid only for completed containers i.e. containers
	 * with state {@link ContainerState#COMPLETE}. 
	 * Otherwise, it returns an ContainerExitStatus.INVALID.
	 * </p>
	 * 
	 * <p>Containers killed by the framework, either due to being released by
	 * the application or being 'lost' due to node failures etc. have a special
	 * exit code of ContainerExitStatus.ABORTED.</p>
	 * 
	 * <p>When threshold number of the nodemanager-local-directories or
	 * threshold number of the nodemanager-log-directories become bad, then
	 * container is not launched and is exited with ContainersExitStatus.DISKS_FAILED.
	 * </p>
	 *  
	 * @return <em>exit status</em> for the container
	 */
	int exitStatus;
	
	/**
	 * Get <em>diagnostic messages</em> for failed containers.
	 * @return <em>diagnostic messages</em> for failed containers
	 */
	String diagnostics;
	public ContainerStatus(){
		this.containerId=new ContainerId();
	}
	public ContainerStatus(ContainerId containerId, ContainerState state,
			int exitStatus, String diagnostics) {
		super();
		this.containerId = containerId;
		this.state = state;
		this.exitStatus = exitStatus;
		this.diagnostics = diagnostics;
	}

	public static ContainerStatus newInstance(ContainerId containerId,
			ContainerState containerState, String diagnostics, int exitStatus) {
		return new ContainerStatus(containerId, containerState,  exitStatus,diagnostics);
	}

	public ContainerId getContainerId() {
		return containerId;
	}

	public void setContainerId(ContainerId containerId) {
		this.containerId = containerId;
	}

	public ContainerState getState() {
		return state;
	}

	public void setState(ContainerState state) {
		this.state = state;
	}

	public int getExitStatus() {
		return exitStatus;
	}

	public void setExitStatus(int exitStatus) {
		this.exitStatus = exitStatus;
	}

	public String getDiagnostics() {
		return diagnostics;
	}

	public void setDiagnostics(String diagnostics) {
		this.diagnostics = diagnostics;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		LOG.debug("container status wirte: "+this.toString());
		containerId.write(out);
		UTF8.writeString(out,state.name());
		UTF8.writeString(out, diagnostics);
		out.writeInt(exitStatus);
	}

	@Override
	public String toString() {
		return "ContainerStatus [containerId=" + containerId + ", state="
				+ state + ", exitStatus=" + exitStatus + ", diagnostics="
				+ diagnostics + "]";
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		containerId.readFields(in);
		state=ContainerState.valueOf(UTF8.readString(in));
		diagnostics=UTF8.readString(in);
		exitStatus=in.readInt();
		
	}
}
