package nodeManager;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import nodeManager.application.Application;
import nodeManager.container.Container;

import resourceManager.scheduler.ApplicationId;
import resourceManager.scheduler.ContainerId;
import resourceManager.scheduler.NodeId;
/**
 * 实际是NMContext
 * @author 无言的雨
 *
 */
public class ContextImpl implements Context {
	private NodeId nodeId = null;
    private final ConcurrentMap<ApplicationId, Application> applications =
        new ConcurrentHashMap<ApplicationId, Application>();
    private final ConcurrentMap<ContainerId, Container> containers =
        new ConcurrentSkipListMap<ContainerId, Container>();

//    private final NMContainerTokenSecretManager containerTokenSecretManager;
//    private final NMTokenSecretManagerInNM nmTokenSecretManager;
//    private ContainerManagementProtocol containerManager;
//    private WebServer webServer;
//    private final NodeHealthStatus nodeHealthStatus = RecordFactoryProvider
//        .getRecordFactory(null).newRecordInstance(NodeHealthStatus.class);

    public ContextImpl(){
    	
    }
//    public NMContext(
//    		NMContainerTokenSecretManager containerTokenSecretManager,
//        NMTokenSecretManagerInNM nmTokenSecretManager
//        ) {
//      this.containerTokenSecretManager = containerTokenSecretManager;
//      this.nmTokenSecretManager = nmTokenSecretManager;
//      this.nodeHealthStatus.setIsNodeHealthy(true);
//      this.nodeHealthStatus.setHealthReport("Healthy");
//      this.nodeHealthStatus.setLastHealthReportTime(System.currentTimeMillis());
//    }

    /**
     * Usable only after ContainerManager is started.
     */
    @Override
    public NodeId getNodeId() {
      return this.nodeId;
    }

//    @Override
//    public int getHttpPort() {
//      return this.webServer.getPort();
//    }

    @Override
    public ConcurrentMap<ApplicationId, Application> getApplications() {
      return this.applications;
    }

    @Override
    public ConcurrentMap<ContainerId, Container> getContainers() {
      return this.containers;
    }

//    @Override
//    public NMContainerTokenSecretManager getContainerTokenSecretManager() {
//      return this.containerTokenSecretManager;
//    }
//    
//    @Override
//    public NMTokenSecretManagerInNM getNMTokenSecretManager() {
//      return this.nmTokenSecretManager;
//    }
//    
//    @Override
//    public NodeHealthStatus getNodeHealthStatus() {
//      return this.nodeHealthStatus;
//    }
//
//    @Override
//    public ContainerManagementProtocol getContainerManager() {
//      return this.containerManager;
//    }
//
//    public void setContainerManager(ContainerManagementProtocol containerManager) {
//      this.containerManager = containerManager;
//    }
//
//    public void setWebServer(WebServer webServer) {
//      this.webServer = webServer;
//    }

    public void setNodeId(NodeId nodeId) {
      this.nodeId = nodeId;
    }
}
