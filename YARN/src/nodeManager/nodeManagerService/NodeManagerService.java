package nodeManager.nodeManagerService;

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

import nodeManager.nodeStatusUpdater.NodeStatusUpdaterImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import protocol.protocol.ContainerManagementProtocol;
import protocol.protocolWritable.StartContainersRequest;
import protocol.protocolWritable.StartContainersResponse;

public class NodeManagerService implements ContainerManagementProtocol {
	private static final Logger LOG = LoggerFactory
			.getLogger(NodeManagerService.class);
	@Override
	synchronized public StartContainersResponse startContainers(
			StartContainersRequest requests) {
		LOG.debug("util check: StartContainersResponse request:"
				+ requests.getRequests().get(0).toString());

		StartContainersResponse response = new StartContainersResponse("ok");
		return response;
	}

	@Override
	public int getProtocolVersion() {
		// TODO Auto-generated method stub
		return 0;
	}

}
