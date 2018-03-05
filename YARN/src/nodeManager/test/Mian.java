package nodeManager.test;

import nodeManager.ContextImpl;
import nodeManager.ContainerExecutor.ContainerExecutor;
import nodeManager.ContainerExecutor.LinuxContainerExecutor;
import nodeManager.containerManagerImpl.ContainerManagerImpl;
import nodeManager.nodeStatusUpdater.NodeStatusUpdater;
import nodeManager.nodeStatusUpdater.NodeStatusUpdaterImpl;
import resourceManager.scheduler.NodeId;
import util.PropertiesFile;
import dispatcher.core.AsyncDispatcher;
import dispatcher.core.Dispatcher;

public class Mian {
	public static void main(String[] args) {
		ContextImpl context = new ContextImpl();
		PropertiesFile pf = new PropertiesFile("config.properties");
		context.setNodeId(new NodeId("localhost", Integer.parseInt(pf
				.get("ContainerManagerPort"))));

		Dispatcher dispatcher = new AsyncDispatcher();
		((AsyncDispatcher) dispatcher).init();
		((AsyncDispatcher) dispatcher).start();
		NodeStatusUpdater updater = new NodeStatusUpdaterImpl(context,
				dispatcher);
		updater.init();
		updater.start();

		ContainerExecutor executor = new LinuxContainerExecutor();
		ContainerManagerImpl containerManagerImpl = new ContainerManagerImpl(
				context, executor);
		containerManagerImpl.init();
		containerManagerImpl.start();

		// Permission.chmod777Permission("/usr/project/test");
		// // System.out.println(new Path(new Path("1") +"/2"));
		// ExecutorService containerLauncher = Executors.newCachedThreadPool();
		// LinuxContainerExecutor exec=new LinuxContainerExecutor();
		// ApplicationId applicationId=new ApplicationId(1, 20170521);
		// Application app=new ApplicationImpl("slient_rain",applicationId);
		// Container container=new ContainerImpl(new Resource(1, 1),new
		// ContainerId(new ApplicationAttemptId(applicationId, 2),
		// 3),"slient_rain") ;
		// ContainerLaunch launch =
		// new ContainerLaunch(
		// // context,
		// // getConfig(),
		// // dispatcher,
		// exec,
		// app,
		// container
		// // dirsHandler,
		// // containerManager
		// );
		// // System.out.println(new ContainerLaunch(exec, app,
		// container).getNMPrivateContainerDir("1", "2"));
		//
		// containerLauncher.submit(launch);
	}

}
