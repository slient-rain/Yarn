package rpc.protobufTest;
import protocol.protocolBuf.GetNewApplicationResoponseProtos.GetNewApplicationResponse;
import rpc.core.VersionedProtocol;

public interface JobSummissionProtocol extends VersionedProtocol{
	public Result getHelloWord(GetNewApplicationResponse say);
}
