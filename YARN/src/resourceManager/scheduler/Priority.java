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
 * 一个int 值
 * The priority assigned to a ResourceRequest or Application or Container 
 * allocation 
 *
 */
public  class Priority implements Comparable<Priority> ,Writable {
	int priority;
	public static final Priority UNDEFINED = newInstance(-1);


	public Priority() {
		super();
		// TODO Auto-generated constructor stub
	}

	public Priority(int priority) {
		super();
		this.priority = priority;
	}

	public static Priority newInstance(int p) {
		return new Priority(p);
	}



	public int getPriority() {
		return priority;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	public static Priority getUndefined() {
		return UNDEFINED;
	}

	@Override
	public int hashCode() {
		final int prime = 517861;
		int result = 9511;
		result = prime * result + getPriority();
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
		Priority other = (Priority) obj;
		if (getPriority() != other.getPriority())
			return false;
		return true;
	}

	@Override
	public int compareTo(Priority other) {
		return other.getPriority() - this.getPriority();
	}

	@Override
	public String toString() {
		return "{Priority: " + getPriority() + "}";
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(priority);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		priority=in.readInt();
	}
}
