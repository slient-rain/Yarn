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

package resourceManager.applicationMasterLauncher;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import protocol.protocol.ContainerManagementProtocol;
import protocol.protocolWritable.ApplicationSubmissionContext;
import protocol.protocolWritable.ContainerLaunchContext;
import protocol.protocolWritable.StartContainerRequest;
import protocol.protocolWritable.StartContainersRequest;
import protocol.protocolWritable.StartContainersResponse;

import dispatcher.core.EventHandler;

import resourceManager.RMApp;
import resourceManager.RMContext;
import resourceManager.scheduler.Allocation;
import resourceManager.scheduler.ApplicationId;
import resourceManager.scheduler.Container;
import resourceManager.scheduler.ContainerId;
import resourceManager.scheduler.NodeId;
import resourceManager.scheduler.Resource;
import rpc.core.RPC;



/**
 * The launch of the AM itself.
 */
public class AMLauncher implements Runnable{

	private static final Logger LOG = LoggerFactory.getLogger(AMLauncher.class);

	public ContainerManagementProtocol containerMgrProxy;

	////  private final RMAppAttempt application;
	////  private final Configuration conf;
	private final AMLauncherEventType eventType;
	private final RMContext rmContext;
	//  private final Container masterContainer;
	private final Allocation allocation;
	//  
	//  @SuppressWarnings("rawtypes")
	private final EventHandler handler;
	//  
	public AMLauncher(
			RMContext rmContext, 
			Allocation allocation,
			//		  RMAppAttempt application,
			AMLauncherEventType eventType
			//      Configuration conf
			) {
		//    this.application = application;
		//    this.conf = conf;
		this.eventType = eventType;
		this.rmContext = rmContext;
		this.handler = rmContext.getDispatcher().getEventHandler();
		//    this.masterContainer = application.getMasterContainer();
		this.allocation=allocation;
	}
	
	public AMLauncher() {
		//    this.application = application;
		//    this.conf = conf;
		this.eventType = null;
		this.rmContext = null;
		this.handler = null;
		//    this.masterContainer = application.getMasterContainer();
		this.allocation=null;
	}

	public void connect() throws IOException {
		//		    ContainerId masterContainerID = masterContainer.getId();
		Container masterContainer = allocation.getContainers().get(0);
		containerMgrProxy = getContainerMgrProxy(masterContainer);
		//		containerMgrProxy = getContainerMgrProxy(null);
	}

	private void launch() {
		try {
			connect();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		StartContainersRequest requests=new StartContainersRequest();
		List<StartContainerRequest> list=new ArrayList<StartContainerRequest>();
		for(Container container : allocation.getContainers()){
			RMApp app=rmContext.getRMApps().get(container.getContainerId().getApplicationAttemptId().getApplicationId());
			StartContainerRequest request=new StartContainerRequest();
			request.setContainerLaunchContext(new ContainerLaunchContext(
					app.getApplicationSubmissionContext().getLocalResouces(),
					new HashMap<String, String>(),
					app.getApplicationSubmissionContext().getCommand(),
					container.getResource(),
					container.getContainerId(),
					container.getContainerId().getApplicationAttemptId().getApplicationId(),
					app.getApplicationSubmissionContext().getUser()
					));
			list.add(request);
		}
		requests.setRequests(list);
		StartContainersResponse response=containerMgrProxy.startContainers(requests);
		LOG.debug("util check: AMLauncher.startContainers()"+response.toString());
		
	}
	

	// Protected. For tests.
	public ContainerManagementProtocol getContainerMgrProxy(
			final Container container) {
		final NodeId node = container.getNodeId();
		final InetSocketAddress containerManagerBindAddress =
				new InetSocketAddress(node.getHost(), node.getPort());
		return (ContainerManagementProtocol)RPC.getProxy(
				ContainerManagementProtocol.class, containerManagerBindAddress, 
				0);
		
	}


	
	@SuppressWarnings("unchecked")
	public void run() {
		launch();	
	}
}
