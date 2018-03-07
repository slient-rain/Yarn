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

package resourceManager.ClientRMService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import protocol.protocol.ApplicationClientProtocol;
import protocol.protocolWritable.ApplicationSubmissionContext;
import protocol.protocolWritable.GetNewApplicationResponse;
import protocol.protocolWritable.ResultStatus;

import resourceManager.RMContext;
import resourceManager.rmAppManager.RMAppManager;
import resourceManager.scheduler.ApplicationId;
import resourceManager.scheduler.Resource;
import rpc.core.RPC;
import rpc.core.Server;
import service.AbstractService;
import util.PropertiesFile;

/**
 * The client interface to the Resource Manager. This module handles all the rpc
 * interfaces to the resource manager from the client.
 */
public class ClientRMService extends AbstractService implements
		ApplicationClientProtocol {
	// private static final ArrayList<ApplicationReport> EMPTY_APPS_REPORT = new
	// ArrayList<ApplicationReport>();
	RMContext rmContext;
	RMAppManager rmAppManager;

	public ClientRMService(String name, RMContext rmContext,
			RMAppManager rmAppManager) {
		super(name);
		this.rmContext = rmContext;
		this.rmAppManager = rmAppManager;
	}

	private static final Logger LOG = LoggerFactory
			.getLogger(ClientRMService.class);

	private Server server;
	InetSocketAddress clientBindAddress;

	@Override
	protected void serviceInit() throws Exception {
		clientBindAddress = getBindAddress();
		super.serviceInit();
	}

	@Override
	protected void serviceStart() throws Exception {
		// Configuration conf = getConfig();
		// YarnRPC rpc = YarnRPC.create(conf);
		this.server = RPC.getServer(this, clientBindAddress.getHostName(),
				clientBindAddress.getPort()
		// conf, this.rmDTSecretManager,
		// conf.getInt(YarnConfiguration.RM_CLIENT_THREAD_COUNT,
		// YarnConfiguration.DEFAULT_RM_CLIENT_THREAD_COUNT)
				);
		this.server.start();
		// clientBindAddress =
		// conf.updateConnectAddr(YarnConfiguration.RM_ADDRESS,
		// server.getListenerAddress());
		super.serviceStart();
	}

	@Override
	protected void serviceStop() throws Exception {
		if (this.server != null) {
			this.server.stop();
		}
		super.serviceStop();
	}

	/**
	 * resourceManager分配一个一个新的applicationId+最大可申请资源量
	 * 
	 * @return
	 */
	public GetNewApplicationResponse getNewApplication() {
		PropertiesFile pf = new PropertiesFile("config.properties");
		int appId = Integer.parseInt(pf.get("applicationIdCounter"));
		long clusterTimestamp = Long.parseLong(pf.get("clusterTimestamp"));
		pf.set("applicationIdCounter", (appId + 1) + "");
		return new GetNewApplicationResponse(new ApplicationId(appId,
				clusterTimestamp), new Resource(1, 1));
	};

	/**
	 * 将Application提交到ResourceManager
	 * 
	 * @param request
	 * @return
	 */
	@Override
	public ResultStatus submitApplication(
			ApplicationSubmissionContext submissionContext) {
		System.out.println(submissionContext.toString());
		ApplicationId applicationId = submissionContext.getApplicationId();		
		String user = submissionContext.getUser();		
		try {
			// call RMAppManager to submit application directly
			rmAppManager.submitApplication(submissionContext,
					System.currentTimeMillis(), false, user);

			LOG.info("Application with id " + applicationId.getId()
					+ " submitted by user " + user);
			// RMAuditLogger.logSuccess(user, AuditConstants.SUBMIT_APP_REQUEST,
			// "ClientRMService", applicationId);
		} catch (Exception e) {
			LOG.error(e.toString());
			return new ResultStatus("failed");
		}
		return new ResultStatus("ok");
	}

	InetSocketAddress getBindAddress() {
		PropertiesFile pf = new PropertiesFile("config.properties");
		return new InetSocketAddress(pf.get("host"), Integer.parseInt(pf
				.get("port")));
	}

	@Override
	public int getProtocolVersion() {
		// TODO Auto-generated method stub
		return 0;
	}

}
