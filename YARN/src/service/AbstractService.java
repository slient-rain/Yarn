package service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractService implements Service {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractService.class);
	/**
	 * Service name.
	 */
	private final String name;

	/** service state */
	private final ServiceStateModel stateModel;

	/**
	 * Service start time. Will be zero until the service is started.
	 */
	private long startTime;
	private final Object stateChangeLock = new Object();

	/**
	 * List of state change listeners; it is final to ensure
	 * that it will never be null.
	 */
	private final List<ServiceStateChangeListener> listeners
	= new ArrayList<ServiceStateChangeListener>();

	/**
	 * The cause of any failure -will be null.
	 * if a service did not stop due to a failure.
	 */
	private Exception failureCause;

	/**
	 * the state in which the service was when it failed.
	 * Only valid when the service is stopped due to a failure
	 */
	private STATE failureState = null;
	/**
	 * object used to co-ordinate {@link #waitForServiceToStop(long)}
	 * across threads.
	 */
	private final AtomicBoolean terminationNotification =
			new AtomicBoolean(false);
	/**
	 * Construct the service.
	 * @param name service name
	 */
	public AbstractService(String name) {
		//LogUtil.loadConfig();
		this.name = name;
		stateModel = new ServiceStateModel(name);
	}

	@Override
	public final STATE getServiceState() {
		return stateModel.getState();
	}

	@Override
	public final synchronized Throwable getFailureCause() {
		return failureCause;
	}

	@Override
	public synchronized STATE getFailureState() {
		return failureState;
	}
	/**
	 * {@inheritDoc}
	 * This invokes {@link #serviceInit}
	 * @param conf the configuration of the service. This must not be null
	 * @throws ServiceStateException if the configuration was null,
	 * the state change not permitted, or something else went wrong
	 */
	@Override
	public void init() {
		if (isInState(STATE.INITED)) {
			return;
		}
		synchronized (stateChangeLock) {
			if (enterState(STATE.INITED) != STATE.INITED) {
				try {
					serviceInit();
				} catch (Exception e) {
					noteFailure(e);
					stopQuietly(LOG, this);
					throw ServiceStateException.convert(e);
				}finally{
					//if the service inited (and isn't now in a later state), notify
					if (isInState(STATE.INITED)) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Service " + getName() + " is inited");
						}
						//if the service ended up here during init,
						//notify the listeners
						notifyListeners();
					}
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 * @throws ServiceStateException if the current service state does not permit
	 * this action
	 */
	@Override
	public void start() {
		if (isInState(STATE.STARTED)) {
			return;
		}
		//enter the started state
		synchronized (stateChangeLock) {
			if (stateModel.enterState(STATE.STARTED) != STATE.STARTED) {
				try {
					startTime = System.currentTimeMillis();
					serviceStart();					
				} catch (Exception e) {
					noteFailure(e);
					stopQuietly(LOG, this);
					throw ServiceStateException.convert(e);
				}finally{
					if (isInState(STATE.STARTED)) {
						//if the service started (and isn't now in a later state), notify
						if (LOG.isDebugEnabled()) {
							LOG.debug("Service " + getName() + " is started");
						}
						notifyListeners();
					}
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void stop() {
		if (isInState(STATE.STOPPED)) {
			return;
		}
		synchronized (stateChangeLock) {
			if (enterState(STATE.STOPPED) != STATE.STOPPED) {
				try {
					serviceStop();
				} catch (Exception e) {
					//stop-time exceptions are logged if they are the first one,
					noteFailure(e);
					throw ServiceStateException.convert(e);
				} finally {
					//report that the service has terminated
					terminationNotification.set(true);
					synchronized (terminationNotification) {
						terminationNotification.notifyAll();
					}
					//notify anything listening for events
					notifyListeners();
					if (isInState(STATE.STOPPED)) {
						//if the service stopped (and isn't now in a later state), notify
						if (LOG.isDebugEnabled()) {
							LOG.debug("Service " + getName() + " is stoped");
						}
					}
					//notify anything listening for events
					notifyListeners();
				}
			} else {
				//already stopped: note it
				if (LOG.isDebugEnabled()) {
					LOG.debug("Ignoring re-entrant call to stop()");
				}
			}
		}
	}

	/**
	 * Enter a state; record this via {@link #recordLifecycleEvent}
	 * and log at the info level.
	 * @param newState the proposed new state
	 * @return the original state
	 * it wasn't already in that state, and the state model permits state re-entrancy.
	 */
	private STATE enterState(STATE newState) {
		assert stateModel != null : "null state in " + name + " " + this.getClass();
		STATE oldState = stateModel.enterState(newState);
		if (oldState != newState) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Service: " + getName() + " entered state " + getServiceState());
			}
		}
		return oldState;
	}

	/**
	 * Notify local and global listeners of state changes.
	 * Exceptions raised by listeners are NOT passed up.
	 */
	private void notifyListeners() {
		try {
			for(ServiceStateChangeListener listener:listeners){
				listener.stateChanged(this);
			}
		} catch (Throwable e) {
			LOG.warn("Exception while notifying listeners of " + this + ": " + e,
					e);
		}
	}

	/**
	 * Failure handling: record the exception
	 * that triggered it -if there was not one already.
	 * Services are free to call this themselves.
	 * @param exception the exception
	 */
	protected final void noteFailure(Exception exception) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("noteFailure " ,exception);
		}
		if (exception == null) {
			//make sure failure logic doesn't itself cause problems
			return;
		}
		//record the failure details, and log it
		synchronized (this) {
			if (failureCause == null) {
				failureCause = exception;
				failureState = getServiceState();
				LOG.info("Service " + getName()
						+ " failed in state " + failureState
						+ "; cause: " + exception,
						exception);
			}
		}
	}
	/**
	 * All initialization code needed by a service.
	 *
	 * This method will only ever be called once during the lifecycle of
	 * a specific service instance.
	 *
	 * Implementations do not need to be synchronized as the logic
	 * in {@link #init(Configuration)} prevents re-entrancy.
	 *
	 * The base implementation checks to see if the subclass has created
	 * a new configuration instance, and if so, updates the base class value
	 * @param conf configuration
	 * @throws Exception on a failure -these will be caught,
	 * possibly wrapped, and wil; trigger a service stop
	 */
	protected void serviceInit() throws Exception {
	}

	/**
	 * Actions called during the INITED to STARTED transition.
	 *
	 * This method will only ever be called once during the lifecycle of
	 * a specific service instance.
	 *
	 * Implementations do not need to be synchronized as the logic
	 * in {@link #start()} prevents re-entrancy.
	 *
	 * @throws Exception if needed -these will be caught,
	 * wrapped, and trigger a service stop
	 */
	protected void serviceStart() throws Exception {

	}

	/**
	 * Actions called during the transition to the STOPPED state.
	 *
	 * This method will only ever be called once during the lifecycle of
	 * a specific service instance.
	 *
	 * Implementations do not need to be synchronized as the logic
	 * in {@link #stop()} prevents re-entrancy.
	 *
	 * Implementations MUST write this to be robust against failures, including
	 * checks for null references -and for the first failure to not stop other
	 * attempts to shut down parts of the service.
	 *
	 * @throws Exception if needed -these will be caught and logged.
	 */
	protected void serviceStop() throws Exception {

	}

	@Override
	public void registerServiceListener(ServiceStateChangeListener l) {
		listeners.add(l);
	}

	@Override
	public void unregisterServiceListener(ServiceStateChangeListener l) {
		listeners.remove(l);
	}

	@Override
	public final boolean isInState(Service.STATE expected) {
		return stateModel.isInState(expected);
	}

	protected static Exception stopQuietly(Logger log, Service service) {
		try {
			stop(service);
		} catch (Exception e) {
			log.warn("When stopping the service " + service.getName()
					+ " : " + e,
					e);
			return e;
		}
		return null;
	}

	/**
	 * Stop a service.
	 * <p/>Do nothing if the service is null or not
	 * in a state in which it can be/needs to be stopped.
	 * <p/>
	 * The service state is checked <i>before</i> the operation begins.
	 * This process is <i>not</i> thread safe.
	 * @param service a service or null
	 */
	private static void stop(Service service) {
		if (service != null) {
			service.stop();
		}
	}
	@Override
	public long getStartTime() {
		return startTime;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public final boolean waitForServiceToStop(long timeout) {
		boolean completed = terminationNotification.get();
		while (!completed) {
			try {
				synchronized(terminationNotification) {
					terminationNotification.wait(timeout);
				}
				// here there has been a timeout, the object has terminated,
				// or there has been a spurious wakeup (which we ignore)
				completed = true;
			} catch (InterruptedException e) {
				// interrupted; have another look at the flag
				completed = terminationNotification.get();
			}
		}
		return terminationNotification.get();
	}
}
