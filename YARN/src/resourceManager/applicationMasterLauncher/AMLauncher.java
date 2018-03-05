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

	private ContainerManagementProtocol containerMgrProxy;

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

	private void connect() throws IOException {
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
		System.out.println("AMLauncher.startContainers()"+response.toString());
		//    ContainerId masterContainerID = masterContainer.getId();
		//    ApplicationSubmissionContext applicationContext =
		//      application.getSubmissionContext();
		//    LOG.info("Setting up container " + masterContainer
		//        + " for AM " + application.getAppAttemptId());  
		//    ContainerLaunchContext launchContext =
		//        createAMContainerLaunchContext(applicationContext, masterContainerID);
		//
		//    StartContainerRequest scRequest =
		//        StartContainerRequest.newInstance(launchContext,
		//          masterContainer.getContainerToken());
		//    List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
		//    list.add(scRequest);
		//    StartContainersRequest allRequests =
		//        StartContainersRequest.newInstance(list);
		//
		//    StartContainersResponse response =
		//        containerMgrProxy.startContainers(allRequests);
		//    if (response.getFailedRequests() != null
		//        && response.getFailedRequests().containsKey(masterContainerID)) {
		//      Throwable t =
		//          response.getFailedRequests().get(masterContainerID).deSerialize();
		//      parseAndThrowException(t);
		//    } else {
		//      LOG.info("Done launching container " + masterContainer + " for AM "
		//          + application.getAppAttemptId());
		//    }
	}

	//  private void cleanup() throws IOException, YarnException {
	//    connect();
	//    ContainerId containerId = masterContainer.getId();
	//    List<ContainerId> containerIds = new ArrayList<ContainerId>();
	//    containerIds.add(containerId);
	//    StopContainersRequest stopRequest =
	//        StopContainersRequest.newInstance(containerIds);
	//    StopContainersResponse response =
	//        containerMgrProxy.stopContainers(stopRequest);
	//    if (response.getFailedRequests() != null
	//        && response.getFailedRequests().containsKey(containerId)) {
	//      Throwable t = response.getFailedRequests().get(containerId).deSerialize();
	//      parseAndThrowException(t);
	//    }
	//  }

	// Protected. For tests.
	protected ContainerManagementProtocol getContainerMgrProxy(
			final Container container) {
		final NodeId node = container.getNodeId();
		final InetSocketAddress containerManagerBindAddress =
				new InetSocketAddress(node.getHost(), node.getPort());
		return (ContainerManagementProtocol)RPC.getProxy(
				ContainerManagementProtocol.class, containerManagerBindAddress, 
				0);
		//
		//    final YarnRPC rpc = YarnRPC.create(conf); // TODO: Don't create again and again.
		//
		//    UserGroupInformation currentUser =
		//        UserGroupInformation.createRemoteUser(containerId
		//            .getApplicationAttemptId().toString());
		//
		//    String user =
		//        rmContext.getRMApps()
		//            .get(containerId.getApplicationAttemptId().getApplicationId())
		//            .getUser();
		//    org.apache.hadoop.yarn.api.records.Token token =
		//        rmContext.getNMTokenSecretManager().createNMToken(
		//            containerId.getApplicationAttemptId(), node, user);
		//    currentUser.addToken(ConverterUtils.convertFromYarn(token,
		//        containerManagerBindAddress));
		//
		//    return currentUser
		//        .doAs(new PrivilegedAction<ContainerManagementProtocol>() {
		//
		//          @Override
		//          public ContainerManagementProtocol run() {
		//            return (ContainerManagementProtocol) rpc.getProxy(
		//                ContainerManagementProtocol.class,
		//                containerManagerBindAddress, conf);
		//          }
		//        });
	}


	//  private ContainerLaunchContext createAMContainerLaunchContext(
	//      ApplicationSubmissionContext applicationMasterContext,
	//      ContainerId containerID) throws IOException {
	//
	//    // Construct the actual Container
	//    ContainerLaunchContext container = 
	//        applicationMasterContext.getAMContainerSpec();
	//    LOG.info("Command to launch container "
	//        + containerID
	//        + " : "
	//        + StringUtils.arrayToString(container.getCommands().toArray(
	//            new String[0])));
	//    
	//    // Finalize the container
	//    setupTokens(container, containerID);
	//    
	//    return container;
	//  }
	//
	//  private void setupTokens(
	//      ContainerLaunchContext container, ContainerId containerID)
	//      throws IOException {
	//    Map<String, String> environment = container.getEnvironment();
	//    environment.put(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV,
	//        application.getWebProxyBase());
	//    // Set AppSubmitTime and MaxAppAttempts to be consumable by the AM.
	//    ApplicationId applicationId =
	//        application.getAppAttemptId().getApplicationId();
	//    environment.put(
	//        ApplicationConstants.APP_SUBMIT_TIME_ENV,
	//        String.valueOf(rmContext.getRMApps()
	//            .get(applicationId)
	//            .getSubmitTime()));
	//    environment.put(ApplicationConstants.MAX_APP_ATTEMPTS_ENV,
	//        String.valueOf(rmContext.getRMApps().get(
	//            applicationId).getMaxAppAttempts()));
	//
	//    Credentials credentials = new Credentials();
	//    DataInputByteBuffer dibb = new DataInputByteBuffer();
	//    if (container.getTokens() != null) {
	//      // TODO: Don't do this kind of checks everywhere.
	//      dibb.reset(container.getTokens());
	//      credentials.readTokenStorageStream(dibb);
	//    }
	//
	//    // Add AMRMToken
	//    Token<AMRMTokenIdentifier> amrmToken = getAMRMToken();
	//    if (amrmToken != null) {
	//      credentials.addToken(amrmToken.getService(), amrmToken);
	//    }
	//    DataOutputBuffer dob = new DataOutputBuffer();
	//    credentials.writeTokenStorageToStream(dob);
	//    container.setTokens(ByteBuffer.wrap(dob.getData(), 0, dob.getLength()));
	//  }
	//
	//  @VisibleForTesting
	//  protected Token<AMRMTokenIdentifier> getAMRMToken() {
	//    return application.getAMRMToken();
	//  }
	//  
	@SuppressWarnings("unchecked")
	public void run() {
		launch();
		//    switch (eventType) {
		//    case LAUNCH:
		//      try {
		//        LOG.info("Launching master" + application.getAppAttemptId());
		//        launch();
		//        handler.handle(new RMAppAttemptEvent(application.getAppAttemptId(),
		//            RMAppAttemptEventType.LAUNCHED));
		//      } catch(Exception ie) {
		//        String message = "Error launching " + application.getAppAttemptId()
		//            + ". Got exception: " + StringUtils.stringifyException(ie);
		//        LOG.info(message);
		//        handler.handle(new RMAppAttemptLaunchFailedEvent(application
		//            .getAppAttemptId(), message));
		//      }
		//      break;
		//    case CLEANUP:
		//      try {
		//        LOG.info("Cleaning master " + application.getAppAttemptId());
		//        cleanup();
		//      } catch(IOException ie) {
		//        LOG.info("Error cleaning master ", ie);
		//      } catch (YarnException e) {
		//        StringBuilder sb = new StringBuilder("Container ");
		//        sb.append(masterContainer.getId().toString());
		//        sb.append(" is not handled by this NodeManager");
		//        if (!e.getMessage().contains(sb.toString())) {
		//          // Ignoring if container is already killed by Node Manager.
		//          LOG.info("Error cleaning master ", e);          
		//        }
		//      }
		//      break;
		//    default:
		//      LOG.warn("Received unknown event-type " + eventType + ". Ignoring.");
		//      break;
		//    }
	}
	//
	//  private void parseAndThrowException(Throwable t) throws YarnException,
	//      IOException {
	//    if (t instanceof YarnException) {
	//      throw (YarnException) t;
	//    } else if (t instanceof InvalidToken) {
	//      throw (InvalidToken) t;
	//    } else {
	//      throw (IOException) t;
	//    }
	//  }
}
