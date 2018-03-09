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

package resourceManager;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import recoverable.stateStore.RMStateStore;
import resourceManager.ClientRMService.ClientRMService;
import resourceManager.appMaster.AppMaster;
import resourceManager.applicationMasterLauncher.ApplicationMasterLauncher;
import resourceManager.scheduler.ApplicationAttemptId;
import resourceManager.scheduler.ApplicationId;
import resourceManager.scheduler.NodeId;
import resourceManager.scheduler.Scheduler;

import dispatcher.core.Dispatcher;

public class RMContextImpl implements RMContext {

	private final Dispatcher rmDispatcher;
	private Scheduler scheduler;
	private final ConcurrentMap<ApplicationId, RMApp> applications = new ConcurrentHashMap<ApplicationId, RMApp>();

	private final ConcurrentMap<NodeId, RMNode> nodes = new ConcurrentHashMap<NodeId, RMNode>();

	private final ConcurrentMap<String, RMNode> inactiveNodes = new ConcurrentHashMap<String, RMNode>();
	public final ConcurrentMap<ApplicationAttemptId, AppMaster> appMasters = new ConcurrentHashMap<ApplicationAttemptId, AppMaster>();

	private RMStateStore stateStore = null;
	private ClientRMService clientRMService;

	public RMContextImpl(Dispatcher rmDispatcher, RMStateStore store) {
		this.rmDispatcher = rmDispatcher;
		this.stateStore = store;
	}

	@Override
	public Dispatcher getDispatcher() {
		return this.rmDispatcher;
	}

	@Override
	public Scheduler getScheduler() {
		return this.scheduler;
	}

	@Override
	public RMStateStore getStateStore() {
		return stateStore;
	}

	@Override
	public ConcurrentMap<ApplicationId, RMApp> getRMApps() {
		return this.applications;
	}

	@Override
	public ConcurrentMap<NodeId, RMNode> getRMNodes() {
		return this.nodes;
	}

	@Override
	public ConcurrentMap<String, RMNode> getInactiveRMNodes() {
		return this.inactiveNodes;
	}

	@Override
	public ClientRMService getClientRMService() {
		return this.clientRMService;
	}

	@Override
	public void setClientRMService(ClientRMService clientRMService) {
		this.clientRMService = clientRMService;
	}

	@Override
	public void setScheduler(Scheduler scheduler) {
		this.scheduler = scheduler;

	}

	@Override
	public ConcurrentMap<ApplicationAttemptId, AppMaster> getAppMasters() {
		return appMasters;
	}

}