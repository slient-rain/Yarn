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

package nodeManager.container;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import protocol.protocolWritable.ApplicationSubmissionContext.LocalResource;
import protocol.protocolWritable.ContainerLaunchContext;

import nodeManager.fs.Path;
import nodeManager.launcher.ContainersLauncherEvent;
import nodeManager.launcher.ContainersLauncherEventType;
import nodeManager.resourceLocalizationService.LocalResourceRequest;
import nodeManager.resourceLocalizationService.LocalResourceVisibility;
import nodeManager.resourceLocalizationService.LocalizerContext;
import nodeManager.resourceLocalizationService.LocalizerResourceRequestEvent;

import resourceManager.scheduler.ContainerId;
import resourceManager.scheduler.ContainerStatus;
import resourceManager.scheduler.Resource;
import resourceManager.scheduler.Scheduler;
import state.InvalidStateTransitonException;
import state.MultipleArcTransition;
import state.SingleArcTransition;
import state.StateMachine;
import state.StateMachineFactory;

import dispatcher.core.Dispatcher;

public class ContainerImpl implements Container {

	private final Lock readLock;
	private final Lock writeLock;
	private final Dispatcher dispatcher;
	// private final Credentials credentials;
	// private final NodeManagerMetrics metrics;
	private final ContainerLaunchContext launchContext;
	// private final ContainerTokenIdentifier containerTokenIdentifier;
	private final ContainerId containerId;
	private final Resource resource;
	private final String user;
	// private int exitCode = ContainerExitStatus.INVALID;
	// private final StringBuilder diagnostics;

	/** The NM-wide configuration - not specific to this container */
	// private final Configuration daemonConf;

	private static final Logger LOG = LoggerFactory
			.getLogger(ContainerImpl.class);

	// private final Map<LocalResourceRequest,List<String>> pendingResources =
	// new HashMap<LocalResourceRequest,List<String>>();
	// private final Map<Path,List<String>> localizedResources =
	// new HashMap<Path,List<String>>();
	// private final List<LocalResourceRequest> publicRsrcs =
	// new ArrayList<LocalResourceRequest>();
	// private final List<LocalResourceRequest> privateRsrcs =
	// new ArrayList<LocalResourceRequest>();
	// private final List<LocalResourceRequest> appRsrcs =
	// new ArrayList<LocalResourceRequest>();

	public ContainerImpl(
	// Configuration conf,
			Dispatcher dispatcher, ContainerLaunchContext launchContext,
			// Credentials creds,
			// NodeManagerMetrics metrics,
			// ContainerTokenIdentifier containerTokenIdentifier
			Resource resource, ContainerId containerId) {
		// this.daemonConf = conf;
		this.dispatcher = dispatcher;
		this.launchContext = launchContext;
		// this.containerTokenIdentifier = containerTokenIdentifier;
		// this.containerId = containerTokenIdentifier.getContainerID();
		// this.resource = containerTokenIdentifier.getResource();
		// this.diagnostics = new StringBuilder();
		// this.credentials = creds;
		// this.metrics = metrics;
		// user = containerTokenIdentifier.getApplicationSubmitter();
		this.containerId = containerId;
		this.resource = resource;
		this.user = launchContext.getUser();

		ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
		this.readLock = readWriteLock.readLock();
		this.writeLock = readWriteLock.writeLock();

		stateMachine = stateMachineFactory.make(this);
	}

	private static final ContainerDoneTransition CONTAINER_DONE_TRANSITION = new ContainerDoneTransition();

	private static final ContainerDiagnosticsUpdateTransition UPDATE_DIAGNOSTICS_TRANSITION = new ContainerDiagnosticsUpdateTransition();
	//
	// State Machine for each container.
	private static StateMachineFactory<ContainerImpl, ContainerState, ContainerEventType, ContainerEvent> stateMachineFactory = new StateMachineFactory<ContainerImpl, ContainerState, ContainerEventType, ContainerEvent>(
			ContainerState.NEW)
			// From NEW State
			.addTransition(
					ContainerState.NEW,
					EnumSet.of(ContainerState.LOCALIZING,
							ContainerState.LOCALIZED,
							ContainerState.LOCALIZATION_FAILED),
					ContainerEventType.INIT_CONTAINER,
					new RequestResourcesTransition())
			.addTransition(ContainerState.NEW, ContainerState.NEW,
					ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
					UPDATE_DIAGNOSTICS_TRANSITION)
			.addTransition(ContainerState.NEW, ContainerState.DONE,
					ContainerEventType.KILL_CONTAINER,
					CONTAINER_DONE_TRANSITION)

			// From LOCALIZING State
			.addTransition(
					ContainerState.LOCALIZING,
					EnumSet.of(ContainerState.LOCALIZING,
							ContainerState.LOCALIZED),
					ContainerEventType.RESOURCE_LOCALIZED,
					new LocalizedTransition())
			.addTransition(ContainerState.LOCALIZING,
					ContainerState.LOCALIZATION_FAILED,
					ContainerEventType.RESOURCE_FAILED,
					new ResourceFailedTransition())
			.addTransition(ContainerState.LOCALIZING,
					ContainerState.LOCALIZING,
					ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
					UPDATE_DIAGNOSTICS_TRANSITION)
			.addTransition(ContainerState.LOCALIZING, ContainerState.KILLING,
					ContainerEventType.KILL_CONTAINER,
					new KillDuringLocalizationTransition())

			// From LOCALIZATION_FAILED State
			.addTransition(ContainerState.LOCALIZATION_FAILED,
					ContainerState.DONE,
					ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
					CONTAINER_DONE_TRANSITION)
			.addTransition(ContainerState.LOCALIZATION_FAILED,
					ContainerState.LOCALIZATION_FAILED,
					ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
					UPDATE_DIAGNOSTICS_TRANSITION)
			// container not launched so kill is a no-op
			.addTransition(ContainerState.LOCALIZATION_FAILED,
					ContainerState.LOCALIZATION_FAILED,
					ContainerEventType.KILL_CONTAINER)
			// container cleanup triggers a release of all resources
			// regardless of whether they were localized or not
			// LocalizedResource handles release event in all states
			.addTransition(ContainerState.LOCALIZATION_FAILED,
					ContainerState.LOCALIZATION_FAILED,
					ContainerEventType.RESOURCE_LOCALIZED)
			.addTransition(ContainerState.LOCALIZATION_FAILED,
					ContainerState.LOCALIZATION_FAILED,
					ContainerEventType.RESOURCE_FAILED)

			// From LOCALIZED State
			.addTransition(ContainerState.LOCALIZED, ContainerState.RUNNING,
					ContainerEventType.CONTAINER_LAUNCHED,
					new LaunchTransition())
			.addTransition(ContainerState.LOCALIZED,
					ContainerState.EXITED_WITH_FAILURE,
					ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
					new ExitedWithFailureTransition(true))
			.addTransition(ContainerState.LOCALIZED, ContainerState.LOCALIZED,
					ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
					UPDATE_DIAGNOSTICS_TRANSITION)
			.addTransition(ContainerState.LOCALIZED, ContainerState.KILLING,
					ContainerEventType.KILL_CONTAINER, new KillTransition())

			// From RUNNING State
			.addTransition(ContainerState.RUNNING,
					ContainerState.EXITED_WITH_SUCCESS,
					ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS,
					new ExitedWithSuccessTransition(true))
			.addTransition(ContainerState.RUNNING,
					ContainerState.EXITED_WITH_FAILURE,
					ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
					new ExitedWithFailureTransition(true))
			.addTransition(ContainerState.RUNNING, ContainerState.RUNNING,
					ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
					UPDATE_DIAGNOSTICS_TRANSITION)
			.addTransition(ContainerState.RUNNING, ContainerState.KILLING,
					ContainerEventType.KILL_CONTAINER, new KillTransition())
			.addTransition(ContainerState.RUNNING,
					ContainerState.EXITED_WITH_FAILURE,
					ContainerEventType.CONTAINER_KILLED_ON_REQUEST,
					new KilledExternallyTransition())

			// From CONTAINER_EXITED_WITH_SUCCESS State
			.addTransition(ContainerState.EXITED_WITH_SUCCESS,
					ContainerState.DONE,
					ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
					CONTAINER_DONE_TRANSITION)
			.addTransition(ContainerState.EXITED_WITH_SUCCESS,
					ContainerState.EXITED_WITH_SUCCESS,
					ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
					UPDATE_DIAGNOSTICS_TRANSITION)
			.addTransition(ContainerState.EXITED_WITH_SUCCESS,
					ContainerState.EXITED_WITH_SUCCESS,
					ContainerEventType.KILL_CONTAINER)

			// From EXITED_WITH_FAILURE State
			.addTransition(ContainerState.EXITED_WITH_FAILURE,
					ContainerState.DONE,
					ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
					CONTAINER_DONE_TRANSITION)
			.addTransition(ContainerState.EXITED_WITH_FAILURE,
					ContainerState.EXITED_WITH_FAILURE,
					ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
					UPDATE_DIAGNOSTICS_TRANSITION)
			.addTransition(ContainerState.EXITED_WITH_FAILURE,
					ContainerState.EXITED_WITH_FAILURE,
					ContainerEventType.KILL_CONTAINER)

			// From KILLING State.
			.addTransition(ContainerState.KILLING,
					ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
					ContainerEventType.CONTAINER_KILLED_ON_REQUEST,
					new ContainerKilledTransition())
			.addTransition(ContainerState.KILLING, ContainerState.KILLING,
					ContainerEventType.RESOURCE_LOCALIZED,
					new LocalizedResourceDuringKillTransition())
			.addTransition(ContainerState.KILLING, ContainerState.KILLING,
					ContainerEventType.RESOURCE_FAILED)
			.addTransition(ContainerState.KILLING, ContainerState.KILLING,
					ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
					UPDATE_DIAGNOSTICS_TRANSITION)
			.addTransition(ContainerState.KILLING, ContainerState.KILLING,
					ContainerEventType.KILL_CONTAINER)
			.addTransition(ContainerState.KILLING,
					ContainerState.EXITED_WITH_SUCCESS,
					ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS,
					new ExitedWithSuccessTransition(false))
			.addTransition(ContainerState.KILLING,
					ContainerState.EXITED_WITH_FAILURE,
					ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
					new ExitedWithFailureTransition(false))
			.addTransition(ContainerState.KILLING, ContainerState.DONE,
					ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
					CONTAINER_DONE_TRANSITION)
			// Handle a launched container during killing stage is a no-op
			// as cleanup container is always handled after launch container
			// event
			// in the container launcher
			.addTransition(ContainerState.KILLING, ContainerState.KILLING,
					ContainerEventType.CONTAINER_LAUNCHED)

			// From CONTAINER_CLEANEDUP_AFTER_KILL State.
			.addTransition(ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
					ContainerState.DONE,
					ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
					CONTAINER_DONE_TRANSITION)
			.addTransition(ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
					ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
					ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
					UPDATE_DIAGNOSTICS_TRANSITION)
			.addTransition(ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
					ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
					ContainerEventType.KILL_CONTAINER)

			// From DONE
			.addTransition(ContainerState.DONE, ContainerState.DONE,
					ContainerEventType.KILL_CONTAINER)
			.addTransition(ContainerState.DONE, ContainerState.DONE,
					ContainerEventType.INIT_CONTAINER)
			.addTransition(ContainerState.DONE, ContainerState.DONE,
					ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
					UPDATE_DIAGNOSTICS_TRANSITION)
			// This transition may result when
			// we notify container of failed localization if localizer thread
			// (for
			// that container) fails for some reason
			.addTransition(ContainerState.DONE, ContainerState.DONE,
					ContainerEventType.RESOURCE_FAILED)

			// create the topology tables
			.installTopology();

	private final StateMachine<ContainerState, ContainerEventType, ContainerEvent> stateMachine;

	//
	// private ContainerState getCurrentState() {
	// switch (stateMachine.getCurrentState()) {
	// case NEW:
	// case LOCALIZING:
	// case LOCALIZATION_FAILED:
	// case LOCALIZED:
	// case RUNNING:
	// case EXITED_WITH_SUCCESS:
	// case EXITED_WITH_FAILURE:
	// case KILLING:
	// case CONTAINER_CLEANEDUP_AFTER_KILL:
	// case CONTAINER_RESOURCES_CLEANINGUP:
	// return ContainerState.RUNNING;
	// case DONE:
	// default:
	// return ContainerState.COMPLETE;
	// }
	// }
	//
	@Override
	public String getUser() {
		this.readLock.lock();
		try {
			return this.user;
		} finally {
			this.readLock.unlock();
		}
	}

	// @Override
	// public Map<Path,List<String>> getLocalizedResources() {
	// this.readLock.lock();
	// try {
	// if (ContainerState.LOCALIZED == getContainerState()) {
	// return localizedResources;
	// } else {
	// return null;
	// }
	// } finally {
	// this.readLock.unlock();
	// }
	// }
	//
	// @Override
	// public Credentials getCredentials() {
	// this.readLock.lock();
	// try {
	// return credentials;
	// } finally {
	// this.readLock.unlock();
	// }
	// }
	//
	// @Override
	// public ContainerState getContainerState() {
	// this.readLock.lock();
	// try {
	// return stateMachine.getCurrentState();
	// } finally {
	// this.readLock.unlock();
	// }
	// }
	//
	// @Override
	// public ContainerLaunchContext getLaunchContext() {
	// this.readLock.lock();
	// try {
	// return launchContext;
	// } finally {
	// this.readLock.unlock();
	// }
	// }
	//
	// @Override
	// public ContainerStatus cloneAndGetContainerStatus() {
	// this.readLock.lock();
	// try {
	// return BuilderUtils.newContainerStatus(this.containerId,
	// getCurrentState(), diagnostics.toString(), exitCode);
	// } finally {
	// this.readLock.unlock();
	// }
	// }

	@Override
	public ContainerId getContainerId() {
		return this.containerId;
	}

	@Override
	public Resource getResource() {
		return this.resource;
	}

	static class ContainerTransition implements
			SingleArcTransition<ContainerImpl, ContainerEvent> {

		@Override
		public void transition(ContainerImpl container, ContainerEvent event) {
			// Just drain the event and change the state.
		}

	}

	/**
	 * State transition when a NEW container receives the INIT_CONTAINER
	 * message.
	 * 
	 * If there are resources to localize, sends a ContainerLocalizationRequest
	 * (INIT_CONTAINER_RESOURCES) to the ResourceLocalizationManager and enters
	 * LOCALIZING state.
	 * 
	 * If there are no resources to localize, sends LAUNCH_CONTAINER event and
	 * enters LOCALIZED state directly.
	 * 
	 * If there are any invalid resources specified, enters LOCALIZATION_FAILED
	 * directly.
	 */
	@SuppressWarnings("unchecked")
	// dispatcher not typed
	static class RequestResourcesTransition
			implements
			MultipleArcTransition<ContainerImpl, ContainerEvent, ContainerState> {
		@Override
		public ContainerState transition(ContainerImpl container,
				ContainerEvent event) {
			final ContainerLaunchContext ctxt = container.launchContext;

			Map<String, LocalResource> cntrRsrc = ctxt.getLocalResources();
			Map<LocalResourceVisibility, Collection<LocalResourceRequest>> req = new HashMap<LocalResourceVisibility, Collection<LocalResourceRequest>>();

			if (!cntrRsrc.isEmpty()) {
				Collection<LocalResourceRequest> collection = new ArrayList<LocalResourceRequest>();
				for (Map.Entry<String, LocalResource> rsrc : cntrRsrc
						.entrySet()) {
					collection.add(new LocalResourceRequest(rsrc.getValue()));
				}
				req.put(LocalResourceVisibility.PUBLIC, collection);
				LOG.debug("util check: RequestResourcesTransition.request"
						+ req);

			}

			container.dispatcher.getEventHandler().handle(
					new LocalizerResourceRequestEvent(req,
							LocalResourceVisibility.PUBLIC,
							new LocalizerContext(container.getUser(), container
									.getContainerId(), container
									.getContainerId().getApplicationAttemptId()
									.getApplicationId())));
			return ContainerState.LOCALIZING;

		}
	}

	/**
	 * Transition when one of the requested resources for this container has
	 * been successfully localized.
	 */
	@SuppressWarnings("unchecked")
	// dispatcher not typed
	static class LocalizedTransition
			implements
			MultipleArcTransition<ContainerImpl, ContainerEvent, ContainerState> {
		@Override
		public ContainerState transition(ContainerImpl container,
				ContainerEvent event) {
			container.dispatcher.getEventHandler().handle(
					new ContainersLauncherEvent(container,
							ContainersLauncherEventType.LAUNCH_CONTAINER));
			// container.metrics.endInitingContainer();
			return ContainerState.LOCALIZED;
		}
	}

	/**
	 * Transition from LOCALIZED state to RUNNING state upon receiving a
	 * CONTAINER_LAUNCHED event
	 */
	@SuppressWarnings("unchecked")
	// dispatcher not typed
	static class LaunchTransition extends ContainerTransition {
		@Override
		public void transition(ContainerImpl container, ContainerEvent event) {
		}
	}

	/**
	 * Transition from RUNNING or KILLING state to EXITED_WITH_SUCCESS state
	 * upon EXITED_WITH_SUCCESS message.
	 */
	@SuppressWarnings("unchecked")
	// dispatcher not typed
	static class ExitedWithSuccessTransition extends ContainerTransition {

		boolean clCleanupRequired;

		public ExitedWithSuccessTransition(boolean clCleanupRequired) {
			this.clCleanupRequired = clCleanupRequired;
		}

		@Override
		public void transition(ContainerImpl container, ContainerEvent event) {
		}
	}

	/**
	 * Transition to EXITED_WITH_FAILURE state upon
	 * CONTAINER_EXITED_WITH_FAILURE state.
	 **/
	@SuppressWarnings("unchecked")
	// dispatcher not typed
	static class ExitedWithFailureTransition extends ContainerTransition {

		boolean clCleanupRequired;

		public ExitedWithFailureTransition(boolean clCleanupRequired) {
			this.clCleanupRequired = clCleanupRequired;
		}

		@Override
		public void transition(ContainerImpl container, ContainerEvent event) {
			// ContainerExitEvent exitEvent = (ContainerExitEvent) event;
			// container.exitCode = exitEvent.getExitCode();
			//
			// // TODO: Add containerWorkDir to the deletion service.
			// // TODO: Add containerOuputDir to the deletion service.
			//
			// if (clCleanupRequired) {
			// container.dispatcher.getEventHandler().handle(
			// new ContainersLauncherEvent(container,
			// ContainersLauncherEventType.CLEANUP_CONTAINER));
			// }
			//
			// container.cleanup();
		}
	}

	/**
	 * Transition to EXITED_WITH_FAILURE upon receiving KILLED_ON_REQUEST
	 */
	static class KilledExternallyTransition extends ExitedWithFailureTransition {
		KilledExternallyTransition() {
			super(true);
		}

		@Override
		public void transition(ContainerImpl container, ContainerEvent event) {
			// super.transition(container, event);
			// container.diagnostics.append("Killed by external signal\n");
		}
	}

	/**
	 * Transition from LOCALIZING to LOCALIZATION_FAILED upon receiving
	 * RESOURCE_FAILED event.
	 */
	static class ResourceFailedTransition implements
			SingleArcTransition<ContainerImpl, ContainerEvent> {
		@Override
		public void transition(ContainerImpl container, ContainerEvent event) {

			// ContainerResourceFailedEvent rsrcFailedEvent =
			// (ContainerResourceFailedEvent) event;
			// container.diagnostics.append(rsrcFailedEvent.getDiagnosticMessage()
			// + "\n");
			//
			//
			// // Inform the localizer to decrement reference counts and cleanup
			// // resources.
			// container.cleanup();
			// container.metrics.endInitingContainer();
		}
	}

	/**
	 * Transition from LOCALIZING to KILLING upon receiving KILL_CONTAINER
	 * event.
	 */
	static class KillDuringLocalizationTransition implements
			SingleArcTransition<ContainerImpl, ContainerEvent> {
		@Override
		public void transition(ContainerImpl container, ContainerEvent event) {
			// Inform the localizer to decrement reference counts and cleanup
			// resources.
			// container.cleanup();
			// container.metrics.endInitingContainer();
			// ContainerKillEvent killEvent = (ContainerKillEvent) event;
			// container.diagnostics.append(killEvent.getDiagnostic()).append("\n");
		}
	}

	/**
	 * Remain in KILLING state when receiving a RESOURCE_LOCALIZED request while
	 * in the process of killing.
	 */
	static class LocalizedResourceDuringKillTransition implements
			SingleArcTransition<ContainerImpl, ContainerEvent> {
		@Override
		public void transition(ContainerImpl container, ContainerEvent event) {
			// ContainerResourceLocalizedEvent rsrcEvent =
			// (ContainerResourceLocalizedEvent) event;
			// List<String> syms =
			// container.pendingResources.remove(rsrcEvent.getResource());
			// if (null == syms) {
			// LOG.warn("Localized unknown resource " + rsrcEvent.getResource()
			// +
			// " for container " + container.containerId);
			// assert false;
			// // fail container?
			// return;
			// }
			// container.localizedResources.put(rsrcEvent.getLocation(), syms);
		}
	}

	/**
	 * Transitions upon receiving KILL_CONTAINER: - LOCALIZED -> KILLING -
	 * RUNNING -> KILLING
	 */
	@SuppressWarnings("unchecked")
	// dispatcher not typed
	static class KillTransition implements
			SingleArcTransition<ContainerImpl, ContainerEvent> {
		@Override
		public void transition(ContainerImpl container, ContainerEvent event) {
			// // Kill the process/process-grp
			// container.dispatcher.getEventHandler().handle(
			// new ContainersLauncherEvent(container,
			// ContainersLauncherEventType.CLEANUP_CONTAINER));
			// ContainerKillEvent killEvent = (ContainerKillEvent) event;
			// container.diagnostics.append(killEvent.getDiagnostic()).append("\n");
		}
	}

	/**
	 * Transition from KILLING to CONTAINER_CLEANEDUP_AFTER_KILL upon receiving
	 * CONTAINER_KILLED_ON_REQUEST.
	 */
	static class ContainerKilledTransition implements
			SingleArcTransition<ContainerImpl, ContainerEvent> {
		@Override
		public void transition(ContainerImpl container, ContainerEvent event) {
			// ContainerExitEvent exitEvent = (ContainerExitEvent) event;
			// container.exitCode = exitEvent.getExitCode();
			//
			// // The process/process-grp is killed. Decrement reference counts
			// and
			// // cleanup resources
			// container.cleanup();
		}
	}

	/**
	 * Handle the following transitions: - NEW -> DONE upon KILL_CONTAINER -
	 * {LOCALIZATION_FAILED, EXITED_WITH_SUCCESS, EXITED_WITH_FAILURE, KILLING,
	 * CONTAINER_CLEANEDUP_AFTER_KILL} -> DONE upon
	 * CONTAINER_RESOURCES_CLEANEDUP
	 */
	static class ContainerDoneTransition implements
			SingleArcTransition<ContainerImpl, ContainerEvent> {
		@Override
		@SuppressWarnings("unchecked")
		public void transition(ContainerImpl container, ContainerEvent event) {
			// container.finished();
			// //if the current state is NEW it means the CONTAINER_INIT was
			// never
			// // sent for the event, thus no need to send the CONTAINER_STOP
			// if (container.getCurrentState()
			// != org.apache.hadoop.yarn.api.records.ContainerState.NEW) {
			// container.dispatcher.getEventHandler().handle(new
			// AuxServicesEvent
			// (AuxServicesEventType.CONTAINER_STOP, container));
			// }
		}
	}

	/**
	 * Update diagnostics, staying in the same state.
	 */
	static class ContainerDiagnosticsUpdateTransition implements
			SingleArcTransition<ContainerImpl, ContainerEvent> {
		@Override
		public void transition(ContainerImpl container, ContainerEvent event) {
			// ContainerDiagnosticsUpdateEvent updateEvent =
			// (ContainerDiagnosticsUpdateEvent) event;
			// container.diagnostics.append(updateEvent.getDiagnosticsUpdate())
			// .append("\n");
		}
	}

	@Override
	public void handle(ContainerEvent event) {
		try {
			this.writeLock.lock();

			ContainerId containerID = event.getContainerID();
			//每个container只处理自己的event
			if (this.containerId.compareTo(containerID) == 0) {
				LOG.debug("Processing " + containerID + " of type "
						+ event.getType());

				ContainerState oldState = stateMachine.getCurrentState();
				ContainerState newState = null;
				try {
					newState = stateMachine
							.doTransition(event.getType(), event);
				} catch (InvalidStateTransitonException e) {
					LOG.warn(
							"Can't handle this event at current state: Current: ["
									+ oldState + "], eventType: ["
									+ event.getType() + "]", e);
				}
				if (oldState != newState) {
					LOG.info("Container " + containerID + " transitioned from "
							+ oldState + " to " + newState);
				}
			}
		} finally {
			this.writeLock.unlock();
		}
	}

	//
	// @Override
	// public String toString() {
	// this.readLock.lock();
	// try {
	// return ConverterUtils.toString(this.containerId);
	// } finally {
	// this.readLock.unlock();
	// }
	// }

	@Override
	public ContainerLaunchContext getLaunchContext() {
		return this.launchContext;
	}
}
