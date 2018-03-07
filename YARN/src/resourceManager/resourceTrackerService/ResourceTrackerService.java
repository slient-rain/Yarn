package resourceManager.resourceTrackerService;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import protocol.protocol.ResourceTrackerProtocol;
import protocol.protocolWritable.NodeHeartbeatRequest;
import protocol.protocolWritable.NodeHeartbeatResponse;
import protocol.protocolWritable.RegisterNodeManagerRequest;
import protocol.protocolWritable.RegisterNodeManagerResponse;
import resourceManager.RMContext;
import resourceManager.RMNode;
import resourceManager.RMNodeEvent;
import resourceManager.RMNodeEventType;
import resourceManager.RMNodeImpl;
import resourceManager.RMNodeStatusEvent;
import resourceManager.scheduler.Node;
import resourceManager.scheduler.NodeId;
import resourceManager.scheduler.Resource;
import rpc.core.RPC;
import rpc.core.Server;
import service.AbstractService;
import util.PropertiesFile;

public class ResourceTrackerService extends AbstractService implements
		ResourceTrackerProtocol {

	private static final Logger LOG = LoggerFactory
			.getLogger(ResourceTrackerService.class);

	// private static final RecordFactory recordFactory =
	// RecordFactoryProvider.getRecordFactory(null);

	private final RMContext rmContext;
	// private final NodesListManager nodesListManager;
	// private final NMLivelinessMonitor nmLivelinessMonitor;
	// private final RMContainerTokenSecretManager containerTokenSecretManager;
	// private final NMTokenSecretManagerInRM nmTokenSecretManager;

	private long nextHeartBeatInterval;
	private Server server;
	private InetSocketAddress resourceTrackerAddress;

	final long DEFAULFNEXTHEARTBEATINTERVAL = 10000;
	private static final NodeHeartbeatResponse resync = new NodeHeartbeatResponse();
	private static final NodeHeartbeatResponse shutDown = new NodeHeartbeatResponse();

	private int minAllocMb;
	private int minAllocVcores;

	static {
		resync.setNodeAction("RESYNC");

		shutDown.setNodeAction("SHUTDOWN");
	}

	public ResourceTrackerService(RMContext rmContext
	// NodesListManager nodesListManager,
	// NMLivelinessMonitor nmLivelinessMonitor,
	// RMContainerTokenSecretManager containerTokenSecretManager,
	// NMTokenSecretManagerInRM nmTokenSecretManager
	) {
		super(ResourceTrackerService.class.getName());
		this.rmContext = rmContext;
		// this.nodesListManager = nodesListManager;
		// this.nmLivelinessMonitor = nmLivelinessMonitor;
		// this.containerTokenSecretManager = containerTokenSecretManager;
		// this.nmTokenSecretManager = nmTokenSecretManager;
	}

	@Override
	protected void serviceInit() throws Exception {
		this.resourceTrackerAddress = getBindAddress();
		// RackResolver.init(conf);
		nextHeartBeatInterval = DEFAULFNEXTHEARTBEATINTERVAL;
		super.serviceInit();
	}

	@Override
	protected void serviceStart() throws Exception {
		this.server = RPC.getServer(this, resourceTrackerAddress.getHostName(),
				resourceTrackerAddress.getPort());
		LOG.debug("util check: resourceTrackerService start listen on port :"+ resourceTrackerAddress.getPort());
		this.server.start();
		super.serviceStart();
	}

	@Override
	protected void serviceStop() throws Exception {
		if (this.server != null) {
			this.server.stop();
		}
		super.serviceStop();
	}

	@SuppressWarnings("unchecked")
	@Override
	public RegisterNodeManagerResponse registerNodeManager(
			RegisterNodeManagerRequest request) {
		LOG.debug("util check: registerNodeManager:" + request.toString());
		NodeId nodeId = request.getNodeId();
		String host = nodeId.getHost();
		int cmPort = nodeId.getPort();
		// int httpPort = request.getHttpPort();
		Resource capability = request.getResource();

		RegisterNodeManagerResponse response = new RegisterNodeManagerResponse();
		response.setNodeAction("NORMAL");

		RMNode rmNode = new RMNodeImpl(nodeId, rmContext, host, cmPort, -1,
				new Node("not Use", 0), capability);
		this.rmContext.getDispatcher().register(RMNodeEventType.class, rmNode);
		RMNode oldNode = this.rmContext.getRMNodes()
				.putIfAbsent(nodeId, rmNode);
		// if (oldNode == null) {
		this.rmContext.getDispatcher().getEventHandler()
				.handle(new RMNodeEvent(nodeId, RMNodeEventType.STARTED));

		return response;
	}

	@SuppressWarnings("unchecked")
	@Override
	public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request) {
		LOG.debug("util check: ResourceTrackerService.nodeHeartbeat()"
				+ request.toString());
		NodeId nodeId = request.getNodeId();
		RMNode rmNode = this.rmContext.getRMNodes().get(nodeId);		
		NodeHeartbeatResponse nodeHeartBeatResponse = new NodeHeartbeatResponse(
				null, null, "NORMAL", nextHeartBeatInterval);
		rmNode.updateNodeHeartbeatResponseForCleanup(nodeHeartBeatResponse);		
		this.rmContext
				.getDispatcher()
				.getEventHandler()
				.handle(new RMNodeStatusEvent(nodeId,
				// remoteNodeStatus.getNodeHealthStatus(),
						request.getContainers(), request
								.getKeepAliveApplications(),
						nodeHeartBeatResponse));

		return nodeHeartBeatResponse;
	}

	

	InetSocketAddress getBindAddress() {
		PropertiesFile pf = new PropertiesFile("config.properties");
		return new InetSocketAddress(pf.get("host"), Integer.parseInt(pf
				.get("ResourceTrackerServicePort")));
	}

	@Override
	public int getProtocolVersion() {
		// TODO Auto-generated method stub
		return 0;
	}
}
