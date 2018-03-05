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

/**
 * Context of the ResourceManager.
 */
public interface RMContext {

  Dispatcher getDispatcher();
  
  RMStateStore getStateStore();

  ConcurrentMap<ApplicationId, RMApp> getRMApps();
  
  ConcurrentMap<String, RMNode> getInactiveRMNodes();

  ConcurrentMap<NodeId, RMNode> getRMNodes();
  Scheduler getScheduler();
  void setScheduler(Scheduler scheduler);
  ConcurrentMap<ApplicationAttemptId, AppMaster> getAppMasters();

//  ApplicationMasterLauncher getApplicationMasterLauncher();
//  AMLivelinessMonitor getAMLivelinessMonitor();
//
//  AMLivelinessMonitor getAMFinishingMonitor();
//
//  ContainerAllocationExpirer getContainerAllocationExpirer();
//  
//  DelegationTokenRenewer getDelegationTokenRenewer();
//
//  AMRMTokenSecretManager getAMRMTokenSecretManager();
//
//  RMContainerTokenSecretManager getContainerTokenSecretManager();
//  
//  NMTokenSecretManagerInRM getNMTokenSecretManager();
//
//  ClientToAMTokenSecretManagerInRM getClientToAMTokenSecretManager();
  
  void setClientRMService(ClientRMService clientRMService);
  
  ClientRMService getClientRMService();
  
//  RMDelegationTokenSecretManager getRMDelegationTokenSecretManager();
//
//  void setRMDelegationTokenSecretManager(
//      RMDelegationTokenSecretManager delegationTokenSecretManager);
}