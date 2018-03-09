package nodeManager.nodeStatusUpdater;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import nodeManager.Context;
import nodeManager.container.Container;
import nodeManager.container.ContainerImpl;
import nodeManager.container.ContainerState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import protocol.protocol.ApplicationClientProtocol;
import protocol.protocol.ResourceTrackerProtocol;
import protocol.protocolWritable.NodeHeartbeatRequest;
import protocol.protocolWritable.NodeHeartbeatResponse;
import protocol.protocolWritable.RegisterNodeManagerRequest;
import protocol.protocolWritable.RegisterNodeManagerResponse;
import dispatcher.core.Dispatcher;
import resourceManager.scheduler.ApplicationId;
import resourceManager.scheduler.ContainerId;
import resourceManager.scheduler.ContainerStatus;
import resourceManager.scheduler.NodeId;
import resourceManager.scheduler.Resource;
import rpc.core.RPC;
import service.AbstractService;
import state.YarnRuntimeException;
import util.PropertiesFile;

public class NodeStatusUpdaterImpl extends AbstractService implements
		NodeStatusUpdater {
	private static final Logger LOG = LoggerFactory
			.getLogger(NodeStatusUpdaterImpl.class);
	// lock
	private final Object heartbeatMonitor = new Object();

	private Context context;
	private final Dispatcher dispatcher;

	private NodeId nodeId;
	private long nextHeartBeatInterval;
	final long DEFAULFNEXTHEARTBEATINTERVAL = 10000;
	private ResourceTrackerProtocol resourceTracker;
	private Resource totalResource;
	private int httpPort;
	private volatile boolean isStopped;

	private Thread statusUpdater;

	public NodeStatusUpdaterImpl(Context context, Dispatcher dispatcher) {
		super(NodeStatusUpdaterImpl.class.getName());
		this.context = context;
		this.dispatcher = dispatcher;
	}

	@Override
	protected void serviceInit() throws Exception {
		getResources();
		super.serviceInit();
	}

	@Override
	protected void serviceStart() throws Exception {

		// NodeManager is the last service to start, so NodeId is available.
		this.nodeId = this.context.getNodeId();
		// this.httpPort = this.context.getHttpPort();
		try {
			// Registration has to be in start so that ContainerManager can get
			// the
			// perNM tokens needed to authenticate ContainerTokens.
			this.resourceTracker = getRMClient();
			registerWithRM();
			super.serviceStart();
			startStatusUpdater();
		} catch (Exception e) {
			String errorMessage = "Unexpected error starting NodeStatusUpdater";
			LOG.error(errorMessage, e);
			throw new YarnRuntimeException(e);
		}
	}

	@Override
	protected void serviceStop() throws Exception {
		// Interrupt the updater.
		this.isStopped = true;
		stopRMProxy();
		super.serviceStop();
	}

	protected void registerWithRM() throws YarnRuntimeException {
		RegisterNodeManagerRequest request = new RegisterNodeManagerRequest();
		// request.setHttpPort(this.httpPort);
		request.setResource(this.totalResource);
		request.setNodeId(this.nodeId);
		RegisterNodeManagerResponse regNMResponse = resourceTracker
				.registerNodeManager(request);
		LOG.debug("util check: registerNodeManager response:"
				+ regNMResponse.toString());
		// this.rmIdentifier = regNMResponse.getRMIdentifier();
		// if the Resourcemanager instructs NM to shutdown.
		if ("SHUTDOWN".equals(regNMResponse.getNodeAction())) {
			String message = "Message from ResourceManager: 尚未实现，在NodeStatusUpdaterImpl";
			// + regNMResponse.getDiagnosticsMessage();
			throw new YarnRuntimeException(
					"Recieved SHUTDOWN signal from Resourcemanager ,Registration of NodeManager failed, "
							+ message);
		}
	}

	protected void startStatusUpdater() {

		Runnable statusUpdaterRunnable = new Runnable() {
			@Override
			public void run() {
				int lastHeartBeatID = 0;
				while (!isStopped) {
					// Send heartbeat
					try {

						NodeHeartbeatRequest request = new NodeHeartbeatRequest();
						List<ContainerStatus> containers = new ArrayList<ContainerStatus>();
						for (Map.Entry<ContainerId, Container> container : context
								.getContainers().entrySet()) {
							ContainerState state = container.getValue()
									.getContainerState();
							if (state == ContainerState.NEW) {
								containers
										.add(new ContainerStatus(
												container.getKey(),
												resourceManager.scheduler.ContainerState.NEW,
												0, "new"));
							} else if (state == ContainerState.RUNNING) {
								containers
										.add(new ContainerStatus(
												container.getKey(),
												resourceManager.scheduler.ContainerState.RUNNING,
												0, "running"));
							} else if (state == ContainerState.EXITED_WITH_SUCCESS) {
								containers
										.add(new ContainerStatus(
												container.getKey(),
												resourceManager.scheduler.ContainerState.COMPLETE,
												0, "complete"));
							}
						}
						
						request.setContainers(containers);
						for(int i=0; i<request.getContainers().size();i++){
							LOG.debug("containerstatus "+i+" "+request.getContainers().get(i));
						}
						request.setKeepAliveApplications(new ArrayList<ApplicationId>());
						request.setNodeId(nodeId);
						NodeHeartbeatResponse response = resourceTracker
								.nodeHeartbeat(request);
						LOG.debug("util check: nodeHeartbeat response :"
								+ response.toString());
						//资源清理工作只是简单的讲容器和应用从列表中删除
						List<ContainerId> containerToClean=response.getContainersToCleanup();
						for(int i=0; i<containerToClean.size();i++){
							ContainerId containerid=containerToClean.get(i);
							ApplicationId appid=containerid.getApplicationAttemptId().getApplicationId();
							context.getContainers().remove(containerid);
							context.getApplications().remove(appid);
						}
						// get next heartbeat interval from response
						nextHeartBeatInterval = response
								.getNextHeartBeatInterval();
						// updateMasterKeys(response);

						if ("SHUTDOWN".equals(response.getNodeAction())) {

						}
						if ("RESYNC".equals(response.getNodeAction())) {

						}

					} finally {
						synchronized (heartbeatMonitor) {
							nextHeartBeatInterval = nextHeartBeatInterval <= 0 ? DEFAULFNEXTHEARTBEATINTERVAL
									: nextHeartBeatInterval;
							try {
								heartbeatMonitor.wait(nextHeartBeatInterval);
							} catch (InterruptedException e) {
								// Do Nothing
							}
						}
					}
				}
			}
		};
		statusUpdater = new Thread(statusUpdaterRunnable, "Node Status Updater");
		statusUpdater.start();
	}

	protected void getResources() {
		PropertiesFile pf = new PropertiesFile("config.properties");

		int memoryMb = Integer.parseInt(pf.get("memoryMb"));
		float vMemToPMem = Float.parseFloat(pf.get("vMemToPMem"));
		int virtualMemoryMb = (int) Math.ceil(memoryMb * vMemToPMem);

		int virtualCores = Integer.parseInt(pf.get("virtualCores"));

		this.totalResource = new Resource();
		this.totalResource.setMemory(memoryMb);
		this.totalResource.setVirtualCores(virtualCores);

		LOG.info("Initialized nodemanager for " + this.nodeId + ":"
				+ " physical-memory=" + memoryMb + " virtual-memory="
				+ virtualMemoryMb + " virtual-cores=" + virtualCores);
	}

	protected void stopRMProxy() {
		if (this.resourceTracker != null) {
			RPC.stopProxy(this.resourceTracker);
		}
	}

	protected ResourceTrackerProtocol getRMClient() {
		return (ResourceTrackerProtocol) RPC.getProxy(
				ResourceTrackerProtocol.class, getRmAddress(), 0);
	}

	private static InetSocketAddress getRmAddress() {
		PropertiesFile pf = new PropertiesFile("config.properties");
		return new InetSocketAddress(pf.get("resourcemanagerhost"), Integer.parseInt(pf
				.get("ResourceTrackerServicePort")));
	}

}
