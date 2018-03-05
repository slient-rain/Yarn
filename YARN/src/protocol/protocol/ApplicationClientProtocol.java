package protocol.protocol;


import java.io.Closeable;


import protocol.protocolWritable.ApplicationSubmissionContext;
import protocol.protocolWritable.GetNewApplicationResponse;
import protocol.protocolWritable.ResultStatus;






import resourceManager.scheduler.ApplicationId;
import rpc.core.VersionedProtocol;


public interface ApplicationClientProtocol extends VersionedProtocol {
	/**
	 * resourceManager分配一个一个新的applicationId+最大可申请资源量
	 * @return
	 */
	public GetNewApplicationResponse  getNewApplication();
	
	/**
	 * 将Application提交到ResourceManager
	 * @param request
	 * @return
	 */
	public ResultStatus submitApplication(ApplicationSubmissionContext request);
}
