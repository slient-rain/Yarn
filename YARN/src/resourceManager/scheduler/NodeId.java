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

import rpc.io.UTF8;
import rpc.io.Writable;

/**
 * host+port
 * <p><code>NodeId</code> is the unique identifier for a node.</p>
 * 
 * <p>It includes the <em>hostname</em> and <em>port</em> to uniquely 
 * identify the node. Thus, it is unique across restarts of any 
 * <code>NodeManager</code>.</p>
 */

public class NodeId implements Comparable<NodeId>, Writable {
	String host;
	int port;
	public NodeId() {
	}
	public NodeId(String host, int port) {
		super();
		this.host = host;
		this.port = port;
	}

	public static NodeId newInstance(String host, int port) {

		return new NodeId(host,port);
	}



	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public String toString() {
		return this.getHost() + ":" + this.getPort();
	}

	@Override
	public int hashCode() {
		final int prime = 493217;
		int result = 8501;
		result = prime * result + this.getHost().hashCode();
		result = prime * result + this.getPort();
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
		NodeId other = (NodeId) obj;
		if (!this.getHost().equals(other.getHost()))
			return false;
		if (this.getPort() != other.getPort())
			return false;
		return true;
	}

	@Override
	public int compareTo(NodeId other) {
		int hostCompare = this.getHost().compareTo(other.getHost());
		if (hostCompare == 0) {
			if (this.getPort() > other.getPort()) {
				return 1;
			} else if (this.getPort() < other.getPort()) {
				return -1;
			}
			return 0;
		}
		return hostCompare;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(port);
		UTF8.writeString(out, host);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		port=in.readInt();
		host=UTF8.readString(in);
		
	}

}
