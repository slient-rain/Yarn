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

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dispatcher.core.Dispatcher;
import dispatcher.core.EventHandler;

import protocol.protocolWritable.ApplicationSubmissionContext;

import recoverable.Recoverable;
import recoverable.stateStore.RMStateStore.ApplicationState;
import recoverable.stateStore.RMStateStore.RMState;
import resourceManager.appMaster.AppMaster;
import resourceManager.scheduler.AppAddedSchedulerEvent;
import resourceManager.scheduler.ApplicationAttemptId;
import resourceManager.scheduler.ApplicationId;
import resourceManager.scheduler.Scheduler;
import state.InvalidStateTransitonException;
import state.MultipleArcTransition;
import state.SingleArcTransition;
import state.StateMachine;
import state.StateMachineFactory;

public class RMAppImpl implements RMApp, Recoverable {

	private static final Logger LOG = LoggerFactory.getLogger(RMAppImpl.class);
	private static final String UNAVAILABLE = "N/A";

	// Immutable fields
	private final ApplicationId applicationId;
	private final RMContext rmContext;
	// private final Configuration conf;
	private final String user;
	private final String name;
	private final ApplicationSubmissionContext submissionContext;
	private final Dispatcher dispatcher;
	// private final Scheduler scheduler;
	// private final ApplicationMasterService masterService;
	// private final StringBuilder diagnostics = new StringBuilder();
	// private final int maxAppAttempts;
	private final ReadLock readLock;
	private final WriteLock writeLock;
	// private final Map<ApplicationAttemptId, RMAppAttempt> attempts
	// = new LinkedHashMap<ApplicationAttemptId, RMAppAttempt>();
	private final long submitTime;
	// private final Set<RMNode> updatedNodes = new HashSet<RMNode>();
	// private final String applicationType;

	// Mutable fields
	private long startTime;
	private long finishTime;
	// private RMAppAttempt currentAttempt;
	private String queue;
	@SuppressWarnings("rawtypes")
	private EventHandler handler;
	private static final FinalTransition FINAL_TRANSITION = new FinalTransition();
	private static final AppFinishedTransition FINISHED_TRANSITION = new AppFinishedTransition();
	// private boolean isAppRemovalRequestSent = false;
	// private RMAppState previousStateAtRemoving;

	private static final StateMachineFactory<RMAppImpl, RMAppState, RMAppEventType, RMAppEvent> stateMachineFactory = new StateMachineFactory<RMAppImpl, RMAppState, RMAppEventType, RMAppEvent>(
			RMAppState.NEW)

			// Transitions from NEW state
			.addTransition(RMAppState.NEW, RMAppState.NEW,
					RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
			.addTransition(RMAppState.NEW, RMAppState.NEW_SAVING,
					RMAppEventType.START, new RMAppSavingTransition())
			.addTransition(RMAppState.NEW, RMAppState.SUBMITTED,
					RMAppEventType.RECOVER, new AppStartedTransition())
			// RMAppEventType.RECOVER, new StartAppAttemptTransition())
			.addTransition(RMAppState.NEW, RMAppState.KILLED,
					RMAppEventType.KILL, new AppKilledTransition())
			.addTransition(RMAppState.NEW, RMAppState.FAILED,
					RMAppEventType.APP_REJECTED, new AppRejectedTransition())

			// Transitions from NEW_SAVING state
			.addTransition(RMAppState.NEW_SAVING, RMAppState.NEW_SAVING,
					RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
			.addTransition(RMAppState.NEW_SAVING, RMAppState.SUBMITTED,
					RMAppEventType.APP_SAVED, new AppStartedTransition())
			// RMAppEventType.RECOVER, new StartAppAttemptTransition())
			.addTransition(RMAppState.NEW_SAVING, RMAppState.KILLED,
					RMAppEventType.KILL, new AppKilledTransition())
			.addTransition(RMAppState.NEW_SAVING, RMAppState.FAILED,
					RMAppEventType.APP_REJECTED, new AppRejectedTransition())

			// Transitions from SUBMITTED state
			.addTransition(RMAppState.SUBMITTED, RMAppState.SUBMITTED,
					RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
			.addTransition(RMAppState.SUBMITTED, RMAppState.FAILED,
					RMAppEventType.APP_REJECTED, new AppRejectedTransition())
			.addTransition(RMAppState.SUBMITTED, RMAppState.ACCEPTED,
					RMAppEventType.APP_ACCEPTED)
			.addTransition(RMAppState.SUBMITTED, RMAppState.KILLED,
					RMAppEventType.KILL, new KillAppAndAttemptTransition())

			// Transitions from ACCEPTED state
			.addTransition(RMAppState.ACCEPTED, RMAppState.ACCEPTED,
					RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
			.addTransition(RMAppState.ACCEPTED, RMAppState.RUNNING,
					RMAppEventType.ATTEMPT_REGISTERED)
			.addTransition(RMAppState.ACCEPTED,
					EnumSet.of(RMAppState.SUBMITTED, RMAppState.FAILED),
					RMAppEventType.ATTEMPT_FAILED,
					new AttemptFailedTransition(RMAppState.SUBMITTED))
			.addTransition(RMAppState.ACCEPTED, RMAppState.KILLED,
					RMAppEventType.KILL, new KillAppAndAttemptTransition())

			// Transitions from RUNNING state
			.addTransition(RMAppState.RUNNING, RMAppState.RUNNING,
					RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
			.addTransition(RMAppState.RUNNING, RMAppState.REMOVING,
					RMAppEventType.ATTEMPT_UNREGISTERED,
					new RMAppRemovingTransition())
			.addTransition(RMAppState.RUNNING, RMAppState.FINISHED,
					RMAppEventType.ATTEMPT_FINISHED, FINISHED_TRANSITION)
			.addTransition(RMAppState.RUNNING,
					EnumSet.of(RMAppState.SUBMITTED, RMAppState.FAILED),
					RMAppEventType.ATTEMPT_FAILED,
					new AttemptFailedTransition(RMAppState.SUBMITTED))
			.addTransition(RMAppState.RUNNING, RMAppState.KILLED,
					RMAppEventType.KILL, new KillAppAndAttemptTransition())

			// Transitions from REMOVING state
			.addTransition(RMAppState.REMOVING, RMAppState.FINISHING,
					RMAppEventType.APP_REMOVED, new RMAppFinishingTransition())
			.addTransition(RMAppState.REMOVING, RMAppState.FINISHED,
					RMAppEventType.ATTEMPT_FINISHED, FINISHED_TRANSITION)
			.addTransition(RMAppState.REMOVING, RMAppState.KILLED,
					RMAppEventType.KILL, new KillAppAndAttemptTransition())
			// ignorable transitions
			.addTransition(RMAppState.REMOVING, RMAppState.REMOVING,
					RMAppEventType.NODE_UPDATE)

			// Transitions from FINISHING state
			.addTransition(RMAppState.FINISHING, RMAppState.FINISHED,
					RMAppEventType.ATTEMPT_FINISHED, FINISHED_TRANSITION)
			.addTransition(RMAppState.FINISHING, RMAppState.FINISHED,
					RMAppEventType.KILL, new KillAppAndAttemptTransition())
			// ignorable transitions
			.addTransition(
					RMAppState.FINISHING,
					RMAppState.FINISHING,
					EnumSet.of(RMAppEventType.NODE_UPDATE,
							RMAppEventType.APP_REMOVED))

			// Transitions from FINISHED state
			// ignorable transitions
			.addTransition(
					RMAppState.FINISHED,
					RMAppState.FINISHED,
					EnumSet.of(RMAppEventType.NODE_UPDATE,
							RMAppEventType.ATTEMPT_UNREGISTERED,
							RMAppEventType.ATTEMPT_FINISHED,
							RMAppEventType.KILL, RMAppEventType.APP_REMOVED))

			// Transitions from FAILED state
			// ignorable transitions
			.addTransition(
					RMAppState.FAILED,
					RMAppState.FAILED,
					EnumSet.of(RMAppEventType.KILL, RMAppEventType.NODE_UPDATE,
							RMAppEventType.APP_SAVED,
							RMAppEventType.APP_REMOVED))

			// Transitions from KILLED state
			// ignorable transitions
			.addTransition(
					RMAppState.KILLED,
					RMAppState.KILLED,
					EnumSet.of(RMAppEventType.APP_ACCEPTED,
							RMAppEventType.APP_REJECTED, RMAppEventType.KILL,
							RMAppEventType.ATTEMPT_FINISHED,
							RMAppEventType.ATTEMPT_FAILED,
							RMAppEventType.ATTEMPT_KILLED,
							RMAppEventType.NODE_UPDATE,
							RMAppEventType.APP_SAVED,
							RMAppEventType.APP_REMOVED))

			.installTopology();

	private final StateMachine<RMAppState, RMAppEventType, RMAppEvent> stateMachine;

	
	public RMAppImpl(ApplicationId applicationId, RMContext rmContext,
			String name, String user, String queue,
			ApplicationSubmissionContext submissionContext,
			long submitTime) {// , String applicationType

		this.applicationId = applicationId;
		this.name = name;
		this.rmContext = rmContext;
		this.dispatcher = rmContext.getDispatcher();
		this.handler = dispatcher.getEventHandler();
		this.user = user;
		this.queue = queue;
		this.submissionContext = submissionContext;
		this.submitTime = submitTime;
		this.startTime = System.currentTimeMillis();
		ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
		this.readLock = lock.readLock();
		this.writeLock = lock.writeLock();

		this.stateMachine = stateMachineFactory.make(this);
	}

	@Override
	public ApplicationId getApplicationId() {
		return this.applicationId;
	}

	@Override
	public ApplicationSubmissionContext getApplicationSubmissionContext() {
		return this.submissionContext;
	}

	@Override
	public RMAppState getState() {
		this.readLock.lock();

		try {
			return this.stateMachine.getCurrentState();
		} finally {
			this.readLock.unlock();
		}
	}

	@Override
	public String getUser() {
		return this.user;
	}

	@Override
	public String getQueue() {
		return this.queue;
	}

	@Override
	public void setQueue(String queue) {
		this.queue = queue;
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public long getFinishTime() {
		this.readLock.lock();

		try {
			return this.finishTime;
		} finally {
			this.readLock.unlock();
		}
	}

	@Override
	public long getStartTime() {
		this.readLock.lock();

		try {
			return this.startTime;
		} finally {
			this.readLock.unlock();
		}
	}

	@Override
	public long getSubmitTime() {
		return this.submitTime;
	}

	@Override
	public void handle(RMAppEvent event) {

		this.writeLock.lock();

		try {
			ApplicationId appID = event.getApplicationId();
			//每个rmapp只处理自己的事件
			if (this.applicationId.compareTo(appID) == 0) {
				LOG.debug("Processing event for " + appID + " of type "
						+ event.getType());
				final RMAppState oldState = getState();
				try {
					/* keep the master in sync with the state machine */
					this.stateMachine.doTransition(event.getType(), event);
				} catch (InvalidStateTransitonException e) {
					LOG.error("Can't handle this event at current state", e);
					/* TODO fail the application on the failed transition */
				}

				if (oldState != getState()) {
					LOG.info(appID + " State change from " + oldState + " to "
							+ getState());
				}
			}
		} finally {
			this.writeLock.unlock();
		}

	}

	@Override
	public void recover(RMState state) throws Exception {
		ApplicationState appState = state.getApplicationState().get(
				getApplicationId());
		// LOG.info("Recovering app: " + getApplicationId() + " with " +
		// + appState.getAttemptCount() + " attempts");
		// for(int i=0; i<appState.getAttemptCount(); ++i) {
		// // create attempt
		// createNewAttempt(false);
		// // recover attempt
		// ((RMAppAttemptImpl) currentAttempt).recover(state);
		// }
		LOG.debug("in RMAppImple.recover  stored state:"
				+ appState.toString());
	}

	private static class RMAppTransition implements
			SingleArcTransition<RMAppImpl, RMAppEvent> {
		public void transition(RMAppImpl app, RMAppEvent event) {
		};

	}

	private static final class RMAppNodeUpdateTransition extends
			RMAppTransition {
		public void transition(RMAppImpl app, RMAppEvent event) {
			// RMAppNodeUpdateEvent nodeUpdateEvent = (RMAppNodeUpdateEvent)
			// event;
			// app.processNodeUpdate(nodeUpdateEvent.getUpdateType(),
			// nodeUpdateEvent.getNode());
		};
	}

	// private static final class StartAppAttemptTransition extends
	// RMAppTransition {
	private static final class AppStartedTransition extends RMAppTransition {

		public void transition(RMAppImpl app, RMAppEvent event) {

			/**
			 * 下面的是从RMappAttempImpl.AttemptStartedTransition中复制过来的
			 */

			System.out.println("in RMAppImpl : AppStartedTransition");
			app.startTime = System.currentTimeMillis();
			ApplicationAttemptId applicationAttemptId = new ApplicationAttemptId(
					app.getApplicationId(), 1);
			app.dispatcher.getEventHandler().handle(
					new AppAddedSchedulerEvent(applicationAttemptId,
							app.submissionContext.getQueue(), app.getUser()));
			AppMaster appMaster = new AppMaster(app.rmContext,
					applicationAttemptId);
			app.rmContext.getAppMasters().put(applicationAttemptId, appMaster);
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			new Thread(appMaster).start();
		}
	}

	private static final class RMAppFinishingTransition extends RMAppTransition {
		@Override
		public void transition(RMAppImpl app, RMAppEvent event) {
			// if (event.getType().equals(RMAppEventType.APP_REMOVED)) {
			// RMAppRemovedEvent removeEvent = (RMAppRemovedEvent) event;
			// if (removeEvent.getRemovedException() != null) {
			// LOG.error(
			// "Failed to remove application: " +
			// removeEvent.getApplicationId(),
			// removeEvent.getRemovedException());
			// ExitUtil.terminate(1, removeEvent.getRemovedException());
			// }
			// }
			// app.finishTime = System.currentTimeMillis();
		}
	}

	private static final class RMAppSavingTransition extends RMAppTransition {
		@Override
		public void transition(RMAppImpl app, RMAppEvent event) {	
			app.rmContext.getStateStore().storeApplication(app);
		}
	}

	private static final class RMAppRemovingTransition extends RMAppTransition {
		@Override
		public void transition(RMAppImpl app, RMAppEvent event) {
			// LOG.info("Removing application with id " + app.applicationId);
			// app.removeApplicationState();
			// app.previousStateAtRemoving = app.getState();
		}
	}

	private static class AppFinishedTransition extends FinalTransition {
		public void transition(RMAppImpl app, RMAppEvent event) {
			// RMAppFinishedAttemptEvent finishedEvent =
			// (RMAppFinishedAttemptEvent)event;
			// app.diagnostics.append(finishedEvent.getDiagnostics());
			// super.transition(app, event);
		};
	}

	private static class AppKilledTransition extends FinalTransition {
		@Override
		public void transition(RMAppImpl app, RMAppEvent event) {
			// app.diagnostics.append("Application killed by user.");
			// super.transition(app, event);
		};
	}

	private static class KillAppAndAttemptTransition extends
			AppKilledTransition {
		@SuppressWarnings("unchecked")
		@Override
		public void transition(RMAppImpl app, RMAppEvent event) {
			// app.handler.handle(new
			// RMAppAttemptEvent(app.currentAttempt.getAppAttemptId(),
			// RMAppAttemptEventType.KILL));
			// super.transition(app, event);
		}
	}

	private static final class AppRejectedTransition extends FinalTransition {
		public void transition(RMAppImpl app, RMAppEvent event) {
			// RMAppRejectedEvent rejectedEvent = (RMAppRejectedEvent)event;
			// app.diagnostics.append(rejectedEvent.getMessage());
			// super.transition(app, event);
		};
	}

	private static class FinalTransition extends RMAppTransition {

		// private Set<NodeId> getNodesOnWhichAttemptRan(RMAppImpl app) {
		// Set<NodeId> nodes = new HashSet<NodeId>();
		// for (RMAppAttempt attempt : app.attempts.values()) {
		// nodes.addAll(attempt.getRanNodes());
		// }
		// return nodes;
		// }

		@SuppressWarnings("unchecked")
		public void transition(RMAppImpl app, RMAppEvent event) {
			// Set<NodeId> nodes = getNodesOnWhichAttemptRan(app);
			// for (NodeId nodeId : nodes) {
			// app.handler.handle(
			// new RMNodeCleanAppEvent(nodeId, app.applicationId));
			// }
			// if (app.getState() != RMAppState.FINISHING) {
			// app.finishTime = System.currentTimeMillis();
			// }
			// // application completely done and remove from state store.
			// app.removeApplicationState();
			//
			// app.handler.handle(
			// new RMAppManagerEvent(app.applicationId,
			// RMAppManagerEventType.APP_COMPLETED));
		};
	}

	private static final class AttemptFailedTransition implements
			MultipleArcTransition<RMAppImpl, RMAppEvent, RMAppState> {

		private final RMAppState initialState;

		public AttemptFailedTransition(RMAppState initialState) {
			this.initialState = initialState;
		}

		@Override
		public RMAppState transition(RMAppImpl operand, RMAppEvent event) {
			// TODO Auto-generated method stub
			return null;
		}

	}
}
