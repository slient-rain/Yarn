package nodeManager.nodeStatusUpdater.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import protocol.protocol.ContainerManagementProtocol;

import resourceManager.scheduler.NodeId;
import rpc.core.RPC;
import rpc.core.Server;
import dispatcher.core.AsyncDispatcher;
import dispatcher.core.Dispatcher;

import nodeManager.ContextImpl;
import nodeManager.nodeManagerService.NodeManagerService;
import nodeManager.nodeStatusUpdater.NodeStatusUpdater;
import nodeManager.nodeStatusUpdater.NodeStatusUpdaterImpl;



public class Main {
	public static void main(String[] args) throws IOException {
		Dispatcher dispatcher=new AsyncDispatcher();
		((AsyncDispatcher)dispatcher).init();
		((AsyncDispatcher)dispatcher).start();
		ContextImpl context=new ContextImpl();
		
		context.setNodeId(new NodeId("localhost",8003));
		NodeStatusUpdaterImpl updater=new NodeStatusUpdaterImpl(context, dispatcher);
		updater.init();
		updater.start();
		ContainerManagementProtocol instance=new NodeManagerService();
		Server server=RPC.getServer(instance, "localhost", 8003);
		server.start();

	}
}
