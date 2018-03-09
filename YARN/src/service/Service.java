package service;

import java.io.IOException;

public interface Service {
	/**
	 * Service states
	 */
	public enum STATE {
		/** Constructed but not initialized */
		NOTINITED(0, "NOTINITED"),

		/** Initialized but not started or stopped */
		INITED(1, "INITED"),

		/** started and not stopped */
		STARTED(2, "STARTED"),

		/** stopped. No further state transitions are permitted */
		STOPPED(3, "STOPPED");

		/**
		 * An integer value for use in array lookup and JMX interfaces.
		 * Although {@link Enum#ordinal()} could do this, explicitly
		 * identify the numbers gives more stability guarantees over time.
		 */
		private final int value;

		/**
		 * A name of the state that can be used in messages
		 */
		private final String statename;

		private STATE(int value, String name) {
			this.value = value;
			this.statename = name;
		}
		/**
		 * Get the integer value of a state
		 * @return the numeric value of the state
		 */
		public int getValue() {
			return value;
		}

		/**
		 * Get the name of a state
		 * @return the state's name
		 */
		@Override
		public String toString() {
			return statename;
		}
	}
	
	
	 /**
	   * Initialize the service.
	   *
	   * The transition MUST be from {@link STATE#NOTINITED} to {@link STATE#INITED}
	   * unless the operation failed and an exception was raised, in which case
	   * {@link #stop()} MUST be invoked and the service enter the state
	   * {@link STATE#STOPPED}.
	   * @param config the configuration of the service
	   * @throws RuntimeException on any failure during the operation

	   */
	  void init();


	  /**
	   * Start the service.
	   *
	   * The transition MUST be from {@link STATE#INITED} to {@link STATE#STARTED}
	   * unless the operation failed and an exception was raised, in which case
	   * {@link #stop()} MUST be invoked and the service enter the state
	   * {@link STATE#STOPPED}.
	   * @throws RuntimeException on any failure during the operation
	   */

	  void start();

	  /**
	   * Stop the service. This MUST be a no-op if the service is already
	   * in the {@link STATE#STOPPED} state. It SHOULD be a best-effort attempt
	   * to stop all parts of the service.
	   *
	   * The implementation must be designed to complete regardless of the service
	   * state, including the initialized/uninitialized state of all its internal
	   * fields.
	   * @throws RuntimeException on any failure during the stop operation
	   */
	  void stop();

	  /**
	   * Register a listener to the service state change events.
	   * If the supplied listener is already listening to this service,
	   * this method is a no-op.
	   * @param listener a new listener
	   */
	  void registerServiceListener(ServiceStateChangeListener listener);

	  /**
	   * Unregister a previously registered listener of the service state
	   * change events. No-op if the listener is already unregistered.
	   * @param listener the listener to unregister.
	   */
	  void unregisterServiceListener(ServiceStateChangeListener listener);

	  /**
	   * Get the name of this service.
	   * @return the service name
	   */
	  String getName();

	  /**
	   * Get the current service state
	   * @return the state of the service
	   */
	  STATE getServiceState();

	  /**
	   * Get the service start time
	   * @return the start time of the service. This will be zero if the service
	   * has not yet been started.
	   */
	  long getStartTime();

	  /**
	   * Query to see if the service is in a specific state.
	   * In a multi-threaded system, the state may not hold for very long.
	   * @param state the expected state
	   * @return true if, at the time of invocation, the service was in that state.
	   */
	  boolean isInState(STATE state);

	  /**
	   * Get the first exception raised during the service failure. If null,
	   * no exception was logged
	   * @return the failure logged during a transition to the stopped state
	   */
	  Throwable getFailureCause();

	  /**
	   * Get the state in which the failure in {@link #getFailureCause()} occurred.
	   * @return the state or null if there was no failure
	   */
	  STATE getFailureState();

	  /**
	   * Block waiting for the service to stop; uses the termination notification
	   * object to do so.
	   *
	   * This method will only return after all the service stop actions
	   * have been executed (to success or failure), or the timeout elapsed
	   * This method can be called before the service is inited or started; this is
	   * to eliminate any race condition with the service stopping before
	   * this event occurs.
	   * @param timeout timeout in milliseconds. A value of zero means "forever"
	   * @return true iff the service stopped in the time period
	   */
	  boolean waitForServiceToStop(long timeout);
}
