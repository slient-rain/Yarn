package service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompositeService extends AbstractService  {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractService.class);

	/**
	 * Policy on shutdown: attempt to close everything (purest) or
	 * only try to close started services (which assumes
	 * that the service implementations may not handle the stop() operation
	 * except when started.
	 * Irrespective of this policy, if a child service fails during
	 * its init() or start() operations, it will have stop() called on it.
	 */
	protected static final boolean STOP_ONLY_STARTED_SERVICES = false;

	private final List<Service> serviceList = new ArrayList<Service>();

	public CompositeService(String name) {
		super(name);
		//LogUtil.loadConfig();
	}

	/**
	 * Get an unmodifiable list of services
	 * @return a list of child services at the time of invocation -
	 * added services will not be picked up.
	 */
	public List<Service> getServices() {
		synchronized (serviceList) {
			return Collections.unmodifiableList(serviceList);
		}
	}

	/**
	 * Add the passed {@link Service} to the list of services managed by this
	 * {@link CompositeService}
	 * @param service the {@link Service} to be added
	 */
	protected void addService(Service service) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Adding service " + service.getName());
		}
		synchronized (serviceList) {
			serviceList.add(service);
		}
	}

	/**
	 * If the passed object is an instance of {@link Service},
	 * add it to the list of services managed by this {@link CompositeService}
	 * @param object
	 * @return true if a service is added, false otherwise.
	 */
	protected boolean addIfService(Object object) {
		if (object instanceof Service) {
			addService((Service) object);
			return true;
		} else {
			return false;
		}
	}

	protected synchronized boolean removeService(Service service) {
		synchronized (serviceList) {
			return serviceList.add(service);
		}
	}

	protected void serviceInit() throws Exception {
		List<Service> services = getServices();
		if (LOG.isDebugEnabled()) {
			LOG.debug(getName() + ": initing services, size=" + services.size());
		}
		for (Service service : services) {
			service.init();
		}
		super.serviceInit();
	}

	protected void serviceStart() throws Exception {
		List<Service> services = getServices();
		if (LOG.isDebugEnabled()) {
			LOG.debug(getName() + ": starting services, size=" + services.size());
		}
		for (Service service : services) {
			// start the service. If this fails that service
			// will be stopped and an exception raised
			service.start();
		}
		super.serviceStart();
	}

	protected void serviceStop() throws Exception {
		//stop all services that were started
		int numOfServicesToStop = serviceList.size();
		if (LOG.isDebugEnabled()) {
			LOG.debug(getName() + ": stopping services, size=" + numOfServicesToStop);
		}
		stop(numOfServicesToStop, STOP_ONLY_STARTED_SERVICES);
		super.serviceStop();
	}

	/**
	 * Stop the services in reverse order
	 *
	 * @param numOfServicesStarted index from where the stop should work
	 * @param stopOnlyStartedServices flag to say "only start services that are
	 * started, not those that are NOTINITED or INITED.
	 * @throws RuntimeException the first exception raised during the
	 * stop process -<i>after all services are stopped</i>
	 */
	private synchronized void stop(int numOfServicesStarted,
			boolean stopOnlyStartedServices) {
		// stop in reverse order of start
		Exception firstException = null;
		List<Service> services = getServices();
		for (int i = numOfServicesStarted - 1; i >= 0; i--) {
			Service service = services.get(i);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Stopping service #" + i + ": " + service);
			}
			STATE state = service.getServiceState();
			//depending on the stop police
			if (state == STATE.STARTED 
					|| (!stopOnlyStartedServices && state == STATE.INITED)) {
				Exception ex = stopQuietly(LOG, service);
				if (ex != null && firstException == null) {
					firstException = ex;
				}
			}
		}
		//after stopping all services, rethrow the first exception raised
		if (firstException != null) {
			throw ServiceStateException.convert(firstException);
		}
	}

	

}
