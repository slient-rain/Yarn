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
		System.out.println("registerNodeManager:" + request.toString());
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
		System.out.println("ResourceTrackerService.nodeHeartbeat()"
				+ request.toString());
		// NodeStatus remoteNodeStatus = request.getNodeStatus();
		// /**
		// * Here is the node heartbeat sequence...
		// * 1. Check if it's a registered node
		// * 2. Check if it's a valid (i.e. not excluded) node
		// * 3. Check if it's a 'fresh' heartbeat i.e. not duplicate heartbeat
		// * 4. Send healthStatus to RMNode
		// */
		//
		NodeId nodeId = request.getNodeId();
		//
		// // 1. Check if it's a registered node
		RMNode rmNode = this.rmContext.getRMNodes().get(nodeId);
		// if (rmNode == null) {
		// /* node does not exist */
		// String message = "Node not found resyncing " +
		// remoteNodeStatus.getNodeId();
		// LOG.info(message);
		// resync.setDiagnosticsMessage(message);
		// return resync;
		// }
		//
		// // Send ping
		// this.nmLivelinessMonitor.receivedPing(nodeId);
		//
		// // 2. Check if it's a valid (i.e. not excluded) node
		// if (!this.nodesListManager.isValidNode(rmNode.getHostName())) {
		// String message =
		// "Disallowed NodeManager nodeId: " + nodeId + " hostname: "
		// + rmNode.getNodeAddress();
		// LOG.info(message);
		// shutDown.setDiagnosticsMessage(message);
		// this.rmContext.getDispatcher().getEventHandler().handle(
		// new RMNodeEvent(nodeId, RMNodeEventType.DECOMMISSION));
		// return shutDown;
		// }

		// 3. Check if it's a 'fresh' heartbeat i.e. not duplicate heartbeat
		// NodeHeartbeatResponse lastNodeHeartbeatResponse =
		// rmNode.getLastNodeHeartBeatResponse();
		// if (remoteNodeStatus.getResponseId() + 1 == lastNodeHeartbeatResponse
		// .getResponseId()) {
		// LOG.info("Received duplicate heartbeat from node "
		// + rmNode.getNodeAddress());
		// return lastNodeHeartbeatResponse;
		// } else if (remoteNodeStatus.getResponseId() + 1 <
		// lastNodeHeartbeatResponse
		// .getResponseId()) {
		// String message =
		// "Too far behind rm response id:"
		// + lastNodeHeartbeatResponse.getResponseId() + " nm response id:"
		// + remoteNodeStatus.getResponseId();
		// LOG.info(message);
		// resync.setDiagnosticsMessage(message);
		// // TODO: Just sending reboot is not enough. Think more.
		// this.rmContext.getDispatcher().getEventHandler().handle(
		// new RMNodeEvent(nodeId, RMNodeEventType.REBOOTING));
		// return resync;
		// }
		//
		// // Heartbeat response
		NodeHeartbeatResponse nodeHeartBeatResponse = new NodeHeartbeatResponse(
				null, null, "NORMAL", nextHeartBeatInterval);
		rmNode.updateNodeHeartbeatResponseForCleanup(nodeHeartBeatResponse);
		//
		// populateKeys(request, nodeHeartBeatResponse);
		//
		// // 4. Send status to RMNode, saving the latest response.
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

	// private void populateKeys(NodeHeartbeatRequest request,
	// NodeHeartbeatResponse nodeHeartBeatResponse) {
	//
	// // Check if node's masterKey needs to be updated and if the currentKey
	// has
	// // roller over, send it across
	//
	// // ContainerTokenMasterKey
	//
	// MasterKey nextMasterKeyForNode =
	// this.containerTokenSecretManager.getNextKey();
	// if (nextMasterKeyForNode != null
	// && (request.getLastKnownContainerTokenMasterKey().getKeyId()
	// != nextMasterKeyForNode.getKeyId())) {
	// nodeHeartBeatResponse.setContainerTokenMasterKey(nextMasterKeyForNode);
	// }
	//
	// // NMTokenMasterKey
	//
	// nextMasterKeyForNode = this.nmTokenSecretManager.getNextKey();
	// if (nextMasterKeyForNode != null
	// && (request.getLastKnownNMTokenMasterKey().getKeyId()
	// != nextMasterKeyForNode.getKeyId())) {
	// nodeHeartBeatResponse.setNMTokenMasterKey(nextMasterKeyForNode);
	// }
	// }
	//
	// /**
	// * resolving the network topology.
	// * @param hostName the hostname of this node.
	// * @return the resolved {@link Node} for this nodemanager.
	// */
	// public static Node resolve(String hostName) {
	// return RackResolver.resolve(hostName);
	// }
	//
	// void refreshServiceAcls(Configuration configuration,
	// PolicyProvider policyProvider) {
	// this.server.refreshServiceAcl(configuration, policyProvider);
	// }

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
