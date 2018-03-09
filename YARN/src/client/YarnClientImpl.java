package client;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import protocol.protocol.ApplicationClientProtocol;
import protocol.protocolWritable.ApplicationSubmissionContext;
import protocol.protocolWritable.GetNewApplicationResponse;





import protocol.protocolWritable.ResultStatus;
import resourceManager.scheduler.ApplicationId;
import resourceManager.scheduler.Scheduler;
import rpc.core.RPC;
import util.PropertiesFile;

public class YarnClientImpl extends YarnClient {
	private static final Logger LOG = LoggerFactory.getLogger(YarnClientImpl.class);
	protected ApplicationClientProtocol rmClient;
	protected InetSocketAddress rmAddress;
	protected long statePollIntervalMillis;

	private static final String ROOT = "root";

	public YarnClientImpl() {
		super(YarnClientImpl.class.getName());
	}

	private static InetSocketAddress getRmAddress() {

		PropertiesFile pf = new PropertiesFile("config.properties");
		//PropertiesFile pf=new PropertiesFile("config.properties");
		return new InetSocketAddress(pf.get("resourcemanagerhost"), Integer.parseInt((pf.get("clientserviceport"))));
	}

	@Override
	protected void serviceInit() throws Exception {
		this.rmAddress = getRmAddress();
		super.serviceInit();
	}

	@Override
	protected void serviceStart() throws Exception {
		rmClient = (ApplicationClientProtocol)RPC.getProxy(ApplicationClientProtocol.class, rmAddress, 0);
		super.serviceStart();
	}

	@Override
	protected void serviceStop() throws Exception {
//		if (this.rmClient != null) {
//			RPC.stopProxy(this.rmClient);
//		}
		super.serviceStop();
	}

	private GetNewApplicationResponse getNewApplication(){
		GetNewApplicationResponse response=rmClient.getNewApplication();
		System.out.println(response.toString());
		 return response;
	}

	@Override
	public YarnClientApplication createApplication(){
		ApplicationSubmissionContext  submissionContext =new ApplicationSubmissionContext();		
		GetNewApplicationResponse newApp = getNewApplication();
		ApplicationId appId = newApp.getApplicationId();
		submissionContext.setApplicationId(appId);
		LOG.debug("util check: new  created ApplicationId " + appId);
		return new YarnClientApplication(newApp, submissionContext);
	}

	@Override
	public void submitApplication(ApplicationSubmissionContext appContext){
		ApplicationId applicationId = appContext.getApplicationId();
		LOG.debug("util check: in YarnClient");
		ResultStatus res=rmClient.submitApplication(appContext);
		LOG.debug("util check: Submitted application " + applicationId + " to ResourceManager"
				+ " at " + this.rmAddress+"result status:"+res.toString());
	}

	

}