package rpc.test;
import rpc.core.VersionedProtocol;

public interface JobSummissionProtocol extends VersionedProtocol{
	public Result getHelloWord(boolean say);
}
