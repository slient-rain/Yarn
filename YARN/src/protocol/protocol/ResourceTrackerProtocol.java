
package protocol.protocol;

import java.io.IOException;

import protocol.protocolWritable.NodeHeartbeatRequest;
import protocol.protocolWritable.NodeHeartbeatResponse;
import protocol.protocolWritable.RegisterNodeManagerRequest;
import protocol.protocolWritable.RegisterNodeManagerResponse;
import rpc.core.VersionedProtocol;


public interface ResourceTrackerProtocol extends VersionedProtocol {
  
  public RegisterNodeManagerResponse registerNodeManager(
      RegisterNodeManagerRequest request) ;

  public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request);

}
