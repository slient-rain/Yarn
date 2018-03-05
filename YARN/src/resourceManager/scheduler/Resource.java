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

import rpc.io.Writable;


/**
 * <p><code>Resource</code> models a set of computer resources in the 
 * cluster.</p>
 * 
 * <p>Currrently it only models <em>memory</em>.</p>
 * 
 * <p>Typically, applications request <code>Resource</code> of suitable
 * capability to run their component tasks.</p>
 * 
 * @see ResourceRequest
 * @see ApplicationMasterProtocol#allocate(org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest)
 */

public class Resource implements Comparable<Resource>,Writable {
	int memory;
	int vCores;
	public Resource(){
		new Resource(0,0);
	}
	public Resource(int memory, int vCores) {
		super();
		this.memory = memory;
		this.vCores = vCores;
	}
	public static Resource newInstance(int memory, int vCores) {   
		return new Resource(memory, vCores);
	}

	public int getMemory() {
		return memory;
	}
	public void setMemory(int memory) {
		this.memory = memory;
	}
	public int getVirtualCores() {
		return vCores;
	}
	public void setVirtualCores(int vCores) {
		this.vCores = vCores;
	}
	@Override
	public int hashCode() {
		final int prime = 263167;
		int result = 3571;
		result = 939769357 + getMemory(); // prime * result = 939769357 initially
		result = prime * result + getVirtualCores();
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof Resource))
			return false;
		Resource other = (Resource) obj;
		if (getMemory() != other.getMemory() || 
				getVirtualCores() != other.getVirtualCores()) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "<memory:" + getMemory() + ", vCores:" + getVirtualCores() + ">";
	}
	@Override
	public int compareTo(Resource o) {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(memory);
		out.writeInt(vCores);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		memory=in.readInt();
		vCores=in.readInt();
		
	}
}
