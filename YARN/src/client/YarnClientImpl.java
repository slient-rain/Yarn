package client;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import protocol.protocol.ApplicationClientProtocol;
import protocol.protocolWritable.ApplicationSubmissionContext;
import protocol.protocolWritable.GetNewApplicationResponse;





import resourceManager.scheduler.ApplicationId;
import resourceManager.scheduler.Scheduler;
import rpc.core.RPC;

import util.PropertiesFile;

public class YarnClientImpl extends YarnClient {
	private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);
	protected ApplicationClientProtocol rmClient;
	protected InetSocketAddress rmAddress;
	protected long statePollIntervalMillis;

	private static final String ROOT = "root";

	public YarnClientImpl() {
		super(YarnClientImpl.class.getName());
	}

	private static InetSocketAddress getRmAddress() {
		PropertiesFile pf=new PropertiesFile("config.properties");
		return new InetSocketAddress(pf.get("host"), Integer.parseInt(pf.get("port")));
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
		return new YarnClientApplication(newApp, submissionContext);
	}

	@Override
	public void submitApplication(ApplicationSubmissionContext appContext){
		ApplicationId applicationId = appContext.getApplicationId();
		rmClient.submitApplication(appContext);
//
//		int pollCount = 0;
//		while (true) {
//			YarnApplicationState state =
//					getApplicationReport(applicationId).getYarnApplicationState();
//			if (!state.equals(YarnApplicationState.NEW) &&
//					!state.equals(YarnApplicationState.NEW_SAVING)) {
//				break;
//			}
//			// Notify the client through the log every 10 poll, in case the client
//			// is blocked here too long.
//			if (++pollCount % 10 == 0) {
//				LOG.info("Application submission is not finished, " +
//						"submitted application " + applicationId +
//						" is still in " + state);
//			}
//			try {
//				Thread.sleep(statePollIntervalMillis);
//			} catch (InterruptedException ie) {
//			}
//		}
//

		System.out.println("Submitted application " + applicationId + " to ResourceManager"
				+ " at " + rmAddress);
	}

	

}