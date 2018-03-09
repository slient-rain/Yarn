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

package nodeManager.launcher;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nodeManager.Context;
import nodeManager.ContainerExecutor.ContainerExecutor;
import nodeManager.application.Application;
import nodeManager.container.Container;
import nodeManager.containerManagerImpl.ContainerManagerImpl;

import dispatcher.core.Dispatcher;
import dispatcher.core.EventHandler;


import resourceManager.scheduler.ContainerId;
import resourceManager.scheduler.Scheduler;
import service.AbstractService;
import state.YarnRuntimeException;



/**
 * The launcher for the containers. This service should be started only after
 * the {@link ResourceLocalizationService} is started as it depends on creation
 * of system directories on the local file-system.
 * 
 */
public class ContainersLauncher extends AbstractService
implements EventHandler<ContainersLauncherEvent> {

	private static final Logger LOG = LoggerFactory.getLogger(ContainersLauncher.class);


	private final Context context;
	private final ContainerExecutor exec;
	private final Dispatcher dispatcher;
	private final ContainerManagerImpl containerManager;

//	private LocalDirsHandlerService dirsHandler;
	public ExecutorService containerLauncher =Executors.newCachedThreadPool();
//			Executors.newCachedThreadPool(
//					new ThreadFactoryBuilder()
//					.setNameFormat("ContainersLauncher #%d")
//					.build());

	public final Map<ContainerId, ContainerLaunch> running =
			Collections.synchronizedMap(new HashMap<ContainerId, ContainerLaunch>());

	public ContainersLauncher(Context context, Dispatcher dispatcher,
			ContainerExecutor exec,
//			LocalDirsHandlerService dirsHandler,
			ContainerManagerImpl containerManager) {
		super("containers-launcher");
		this.exec = exec;
		this.context = context;
		this.dispatcher = dispatcher;
//		this.dirsHandler = dirsHandler;
		this.containerManager = containerManager;
	}

	@Override
	protected void serviceInit() throws Exception {
//		try {
//			//TODO Is this required?
//			FileContext.getLocalFSFileContext(conf);
//		} catch (UnsupportedFileSystemException e) {
//			throw new YarnRuntimeException("Failed to start ContainersLauncher", e);
//		}
		super.serviceInit();
	}

	@Override
	protected  void serviceStop() throws Exception {
//		containerLauncher.shutdownNow();
		super.serviceStop();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void handle(ContainersLauncherEvent event) {
		// TODO: ContainersLauncher launches containers one by one!!
		Container container = event.getContainer();
		ContainerId containerId = container.getContainerId();
		LOG.debug("Processing " + containerId + " of type "
				+ event.getType());
		switch (event.getType()) {
		case LAUNCH_CONTAINER:
			Application app =context.getApplications().get(
					containerId.getApplicationAttemptId().getApplicationId());

			ContainerLaunch launch =
					new ContainerLaunch(
//							context, 
//							getConfig(), 
							dispatcher, 
							exec, 
							app,
							event.getContainer()
//							dirsHandler, 
//							containerManager
							);
			containerLauncher.submit(launch);
			running.put(containerId, launch);
			break;
		case CLEANUP_CONTAINER:
			ContainerLaunch launcher = running.remove(containerId);
			if (launcher == null) {
				// Container not launched. So nothing needs to be done.
				return;
			}
			break;
		}
	}

}
