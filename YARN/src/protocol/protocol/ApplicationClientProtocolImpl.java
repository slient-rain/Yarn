package protocol.protocol;


import java.io.Closeable;


import protocol.protocolWritable.ApplicationSubmissionContext;
import protocol.protocolWritable.GetNewApplicationResponse;
import protocol.protocolWritable.ResultStatus;



import resourceManager.scheduler.ApplicationId;
import resourceManager.scheduler.Resource;
import rpc.core.VersionedProtocol;
import util.PropertiesFile;


public class ApplicationClientProtocolImpl implements ApplicationClientProtocol {
	/**
	 * resourceManager分配一个一个新的applicationId+最大可申请资源量
	 * @return
	 */
	public GetNewApplicationResponse  getNewApplication(){
		PropertiesFile pf=new PropertiesFile("config.properties");
		int appId=Integer.parseInt(pf.get("applicationIdCounter"));
		long clusterTimestamp=Long.parseLong(pf.get("clusterTimestamp"));
		pf.set("applicationIdCounter", (appId+1)+"");
		return new GetNewApplicationResponse(new ApplicationId(appId, clusterTimestamp),new Resource(1,1));
	};
	
	/**
	 * 将Application提交到ResourceManager
	 * @param request
	 * @return
	 */
	public ResultStatus submitApplication(ApplicationSubmissionContext request){
		System.out.println(request.toString());
		return new ResultStatus("ok");
	};

	@Override
	public int getProtocolVersion() {
		// TODO Auto-generated method stub
		return 0;
	}
}
