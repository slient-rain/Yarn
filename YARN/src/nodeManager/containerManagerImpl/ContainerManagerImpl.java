package nodeManager.containerManagerImpl;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import nodeManager.Context;
import nodeManager.ContainerExecutor.ContainerExecutor;
import nodeManager.application.Application;
import nodeManager.application.ApplicationImpl;
import nodeManager.container.Container;
import nodeManager.container.ContainerEvent;
import nodeManager.container.ContainerEventType;
import nodeManager.container.ContainerImpl;
import nodeManager.launcher.ContainersLauncher;
import nodeManager.launcher.ContainersLauncherEventType;
import nodeManager.resourceLocalizationService.LocalizerEventType;
import nodeManager.resourceLocalizationService.ResourceLocalizationService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import protocol.protocol.ContainerManagementProtocol;
import protocol.protocolWritable.ContainerLaunchContext;
import protocol.protocolWritable.StartContainerRequest;
import protocol.protocolWritable.StartContainersRequest;
import protocol.protocolWritable.StartContainersResponse;
import resourceManager.scheduler.ApplicationId;
import resourceManager.scheduler.ContainerId;
import resourceManager.scheduler.Resource;
import rpc.core.RPC;
import rpc.core.Server;
import service.CompositeService;
import util.PropertiesFile;
import dispatcher.core.AsyncDispatcher;
import dispatcher.core.EventHandler;

public class ContainerManagerImpl extends CompositeService implements
		ContainerManagementProtocol {// ServiceStateChangeListener,
										// ,EventHandler<ContainerManagerEvent>

	private static final Logger LOG = LoggerFactory
			.getLogger(ContainerManagerImpl.class);

	final Context context;

	private Server server;
	private InetSocketAddress bindAddress;
	private final ContainersLauncher containersLauncher;
	protected final AsyncDispatcher dispatcher;
	private boolean serviceStopped = false;
	private final ReadLock readLock;
	private final WriteLock writeLock;
	private ResourceLocalizationService resourceLocalizationService;

	public ContainerManagerImpl(Context context, ContainerExecutor exec
	// DeletionService deletionContext, NodeStatusUpdater nodeStatusUpdater,
	// NodeManagerMetrics metrics, ApplicationACLsManager aclsManager,
	// LocalDirsHandlerService dirsHandler
	) {
		super(ContainerManagerImpl.class.getName());
		this.context = context;
		// this.dirsHandler = dirsHandler;
		//
		// ContainerManager level dispatcher.
		dispatcher = new AsyncDispatcher();
		resourceLocalizationService = new ResourceLocalizationService(
				dispatcher);
		containersLauncher = createContainersLauncher(context, exec);
		addService(containersLauncher);
		dispatcher.register(ContainerEventType.class,
				new ContainerEventDispatcher());
		dispatcher.register(LocalizerEventType.class,
				resourceLocalizationService);
		dispatcher.register(ContainersLauncherEventType.class,
				containersLauncher);

		addService(dispatcher);
		addService(resourceLocalizationService);
		ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
		this.readLock = lock.readLock();
		this.writeLock = lock.writeLock();
	}

	//
	@Override
	public void serviceInit() throws Exception {
		bindAddress = getBindAddress();
		super.serviceInit();
	}

	
	protected ContainersLauncher createContainersLauncher(Context context,
			ContainerExecutor exec) {
		return new ContainersLauncher(context, this.dispatcher, exec,
		// dirsHandler,
				this);
	}

	//
	@Override
	protected void serviceStart() throws Exception {

		

		this.server = RPC.getServer(this, bindAddress.getHostName(),
				bindAddress.getPort());
		server.start();
		super.serviceStart();
	}

	@Override
	public void serviceStop() throws Exception {
		if (server != null) {
			server.stop();
		}
		super.serviceStop();
	}

	
	@Override
	public StartContainersResponse startContainers(
			StartContainersRequest requests) {
		System.out.println("NodeManagerService.StartContainersResponse():"
				+ requests.getRequests().get(0).toString());
		for (StartContainerRequest request : requests.getRequests()) {
			ContainerId containerId = null;
			startContainerInternal(
			request);
		}
		return new StartContainersResponse("ok");
	}

	@SuppressWarnings("unchecked")
	private void startContainerInternal(
			StartContainerRequest request) {
		ContainerLaunchContext launchContext = request
				.getContainerLaunchContext();
		ContainerId containerId = launchContext.getContainerId();
		String containerIdStr = containerId.toString();
		String user = launchContext.getUser();
		Resource resource = launchContext.getResource();

		LOG.info("Start request for " + containerIdStr + " by user " + user);

		Container container = new ContainerImpl(
		// getConfig(),
				this.dispatcher, launchContext, resource, containerId
		);
		ApplicationId applicationID = containerId.getApplicationAttemptId()
				.getApplicationId();
		if (context.getContainers().putIfAbsent(containerId, container) != null) {
			System.err.println(user + "ContainerManagerImpl"
					+ "Container already running on this node!"
					+ applicationID.toString() + containerId.toString());
			// throw RPCUtil.getRemoteException("Container " + containerIdStr
			// + " already is running on this node!!");
		}

		this.readLock.lock();
		try {
			if (!serviceStopped) {
				// Create the application
				Application application = new ApplicationImpl(dispatcher,
				// this.aclsManager,
						user, applicationID,
						// credentials,
						context);
				if (null == context.getApplications().putIfAbsent(
						applicationID, application)) {
					LOG.info("Creating a new application reference for app "
							+ applicationID);

					// dispatcher.getEventHandler().handle(
					// new ApplicationInitEvent(applicationID, container
					// .getLaunchContext().getApplicationACLs()));
				}
				/**
				 * 自己加的
				 */
				application.getContainers().put(containerId, container);
				dispatcher.getEventHandler().handle(
						new ContainerEvent(containerId,
								ContainerEventType.INIT_CONTAINER));
			} else {
				System.err
						.println("Container start failed as the NodeManager is "
								+ "in the process of shutting down");
			}
		} finally {
			this.readLock.unlock();
		}
	}

	//
	// protected ContainerTokenIdentifier verifyAndGetContainerTokenIdentifier(
	// org.apache.hadoop.yarn.api.records.Token token,
	// ContainerTokenIdentifier containerTokenIdentifier) throws YarnException,
	// InvalidToken {
	// byte[] password =
	// context.getContainerTokenSecretManager().retrievePassword(
	// containerTokenIdentifier);
	// byte[] tokenPass = token.getPassword().array();
	// if (password == null || tokenPass == null
	// || !Arrays.equals(password, tokenPass)) {
	// throw new InvalidToken(
	// "Invalid container token used for starting container on : "
	// + context.getNodeId().toString());
	// }
	// return containerTokenIdentifier;
	// }
	//
	// @Private
	// @VisibleForTesting
	// protected void updateNMTokenIdentifier(NMTokenIdentifier
	// nmTokenIdentifier)
	// throws InvalidToken {
	// context.getNMTokenSecretManager().appAttemptStartContainer(
	// nmTokenIdentifier);
	// }
	//
	// private Credentials parseCredentials(ContainerLaunchContext
	// launchContext)
	// throws YarnException {
	// Credentials credentials = new Credentials();
	// // //////////// Parse credentials
	// ByteBuffer tokens = launchContext.getTokens();
	//
	// if (tokens != null) {
	// DataInputByteBuffer buf = new DataInputByteBuffer();
	// tokens.rewind();
	// buf.reset(tokens);
	// try {
	// credentials.readTokenStorageStream(buf);
	// if (LOG.isDebugEnabled()) {
	// for (Token<? extends TokenIdentifier> tk : credentials.getAllTokens()) {
	// LOG.debug(tk.getService() + " = " + tk.toString());
	// }
	// }
	// } catch (IOException e) {
	// throw RPCUtil.getRemoteException(e);
	// }
	// }
	// // //////////// End of parsing credentials
	// return credentials;
	// }
	//
	// /**
	// * Stop a list of containers running on this NodeManager.
	// */
	// @Override
	// public StopContainersResponse stopContainers(StopContainersRequest
	// requests)
	// throws YarnException, IOException {
	//
	// List<ContainerId> succeededRequests = new ArrayList<ContainerId>();
	// Map<ContainerId, SerializedException> failedRequests =
	// new HashMap<ContainerId, SerializedException>();
	// UserGroupInformation remoteUgi = getRemoteUgi();
	// NMTokenIdentifier identifier = selectNMTokenIdentifier(remoteUgi);
	// for (ContainerId id : requests.getContainerIds()) {
	// try {
	// stopContainerInternal(identifier, id);
	// succeededRequests.add(id);
	// } catch (YarnException e) {
	// failedRequests.put(id, SerializedException.newInstance(e));
	// }
	// }
	// return StopContainersResponse
	// .newInstance(succeededRequests, failedRequests);
	// }
	//
	// @SuppressWarnings("unchecked")
	// private void stopContainerInternal(NMTokenIdentifier nmTokenIdentifier,
	// ContainerId containerID) throws YarnException {
	// String containerIDStr = containerID.toString();
	// Container container = this.context.getContainers().get(containerID);
	// LOG.info("Stopping container with container Id: " + containerIDStr);
	// authorizeGetAndStopContainerRequest(containerID, container, true,
	// nmTokenIdentifier);
	//
	// if (container == null) {
	// if (!nodeStatusUpdater.isContainerRecentlyStopped(containerID)) {
	// throw RPCUtil.getRemoteException("Container " + containerIDStr
	// + " is not handled by this NodeManager");
	// }
	// } else {
	// dispatcher.getEventHandler().handle(
	// new ContainerKillEvent(containerID,
	// "Container killed by the ApplicationMaster."));
	//
	// NMAuditLogger.logSuccess(container.getUser(),
	// AuditConstants.STOP_CONTAINER, "ContainerManageImpl", containerID
	// .getApplicationAttemptId().getApplicationId(), containerID);
	//
	// // TODO: Move this code to appropriate place once kill_container is
	// // implemented.
	// nodeStatusUpdater.sendOutofBandHeartBeat();
	// }
	// }
	//
	// /**
	// * Get a list of container statuses running on this NodeManager
	// */
	// @Override
	// public GetContainerStatusesResponse getContainerStatuses(
	// GetContainerStatusesRequest request) throws YarnException, IOException {
	//
	// List<ContainerStatus> succeededRequests = new
	// ArrayList<ContainerStatus>();
	// Map<ContainerId, SerializedException> failedRequests =
	// new HashMap<ContainerId, SerializedException>();
	// UserGroupInformation remoteUgi = getRemoteUgi();
	// NMTokenIdentifier identifier = selectNMTokenIdentifier(remoteUgi);
	// for (ContainerId id : request.getContainerIds()) {
	// try {
	// ContainerStatus status = getContainerStatusInternal(id, identifier);
	// succeededRequests.add(status);
	// } catch (YarnException e) {
	// failedRequests.put(id, SerializedException.newInstance(e));
	// }
	// }
	// return GetContainerStatusesResponse.newInstance(succeededRequests,
	// failedRequests);
	// }
	//
	// private ContainerStatus getContainerStatusInternal(ContainerId
	// containerID,
	// NMTokenIdentifier nmTokenIdentifier) throws YarnException {
	// String containerIDStr = containerID.toString();
	// Container container = this.context.getContainers().get(containerID);
	//
	// LOG.info("Getting container-status for " + containerIDStr);
	// authorizeGetAndStopContainerRequest(containerID, container, false,
	// nmTokenIdentifier);
	//
	// if (container == null) {
	// if (nodeStatusUpdater.isContainerRecentlyStopped(containerID)) {
	// throw RPCUtil.getRemoteException("Container " + containerIDStr
	// + " was recently stopped on node manager.");
	// } else {
	// throw RPCUtil.getRemoteException("Container " + containerIDStr
	// + " is not handled by this NodeManager");
	// }
	// }
	// ContainerStatus containerStatus = container.cloneAndGetContainerStatus();
	// LOG.info("Returning " + containerStatus);
	// return containerStatus;
	// }
	//
	// @Private
	// @VisibleForTesting
	// protected void authorizeGetAndStopContainerRequest(ContainerId
	// containerId,
	// Container container, boolean stopRequest, NMTokenIdentifier identifier)
	// throws YarnException {
	// /*
	// * For get/stop container status; we need to verify that 1) User (NMToken)
	// * application attempt only has started container. 2) Requested
	// containerId
	// * belongs to the same application attempt (NMToken) which was used.
	// (Note:-
	// * This will prevent user in knowing another application's containers).
	// */
	//
	// if ((!identifier.getApplicationAttemptId().equals(
	// containerId.getApplicationAttemptId()))
	// || (container != null && !identifier.getApplicationAttemptId().equals(
	// container.getContainerId().getApplicationAttemptId()))) {
	// if (stopRequest) {
	// LOG.warn(identifier.getApplicationAttemptId()
	// + " attempted to stop non-application container : "
	// + container.getContainerId().toString());
	// NMAuditLogger.logFailure("UnknownUser", AuditConstants.STOP_CONTAINER,
	// "ContainerManagerImpl", "Trying to stop unknown container!",
	// identifier.getApplicationAttemptId().getApplicationId(),
	// container.getContainerId());
	// } else {
	// LOG.warn(identifier.getApplicationAttemptId()
	// + " attempted to get status for non-application container : "
	// + container.getContainerId().toString());
	// }
	// }
	//
	// }
	//
	class ContainerEventDispatcher implements EventHandler<ContainerEvent> {
		@Override
		public void handle(ContainerEvent event) {
			Map<ContainerId, Container> containers = ContainerManagerImpl.this.context
					.getContainers();
			Container c = containers.get(event.getContainerID());
			if (c != null) {
				c.handle(event);
			} else {
				LOG.warn("Event " + event + " sent to absent container "
						+ event.getContainerID());
			}
		}
	}

	//
	// class ApplicationEventDispatcher implements
	// EventHandler<ApplicationEvent> {
	//
	// @Override
	// public void handle(ApplicationEvent event) {
	// Application app =
	// ContainerManagerImpl.this.context.getApplications().get(
	// event.getApplicationID());
	// if (app != null) {
	// app.handle(event);
	// } else {
	// LOG.warn("Event " + event + " sent to absent application "
	// + event.getApplicationID());
	// }
	// }
	// }
	//
	// @SuppressWarnings("unchecked")
	// @Override
	// public void handle(ContainerManagerEvent event) {
	// switch (event.getType()) {
	// case FINISH_APPS:
	// CMgrCompletedAppsEvent appsFinishedEvent =
	// (CMgrCompletedAppsEvent) event;
	// for (ApplicationId appID : appsFinishedEvent.getAppsToCleanup()) {
	// String diagnostic = "";
	// if (appsFinishedEvent.getReason() ==
	// CMgrCompletedAppsEvent.Reason.ON_SHUTDOWN) {
	// diagnostic = "Application killed on shutdown";
	// } else if (appsFinishedEvent.getReason() ==
	// CMgrCompletedAppsEvent.Reason.BY_RESOURCEMANAGER) {
	// diagnostic = "Application killed by ResourceManager";
	// }
	// this.dispatcher.getEventHandler().handle(
	// new ApplicationFinishEvent(appID,
	// diagnostic));
	// }
	// break;
	// case FINISH_CONTAINERS:
	// CMgrCompletedContainersEvent containersFinishedEvent =
	// (CMgrCompletedContainersEvent) event;
	// for (ContainerId container : containersFinishedEvent
	// .getContainersToCleanup()) {
	// this.dispatcher.getEventHandler().handle(
	// new ContainerKillEvent(container,
	// "Container Killed by ResourceManager"));
	// }
	// break;
	// default:
	// throw new YarnRuntimeException(
	// "Got an unknown ContainerManagerEvent type: " + event.getType());
	// }
	// }
	//
	// public void setBlockNewContainerRequests(boolean
	// blockNewContainerRequests) {
	// this.blockNewContainerRequests.set(blockNewContainerRequests);
	// }
	//
	// @Private
	// @VisibleForTesting
	// public boolean getBlockNewContainerRequestsStatus() {
	// return this.blockNewContainerRequests.get();
	// }
	//
	// @Override
	// public void stateChanged(Service service) {
	// // TODO Auto-generated method stub
	// }
	//
	// public Context getContext() {
	// return this.context;
	// }
	//
	// public Map<String, ByteBuffer> getAuxServiceMetaData() {
	// return this.auxiliaryServices.getMetaData();
	// }

	InetSocketAddress getBindAddress() {
		PropertiesFile pf = new PropertiesFile("config.properties");
		return new InetSocketAddress(pf.get("host"), Integer.parseInt(pf
				.get("ContainerManagerPort")));
	}

	@Override
	public int getProtocolVersion() {
		// TODO Auto-generated method stub
		return 0;
	}
}
