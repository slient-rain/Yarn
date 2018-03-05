package resourceManager.rmAppManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import protocol.protocolWritable.ApplicationSubmissionContext;

import dispatcher.core.EventHandler;

import resourceManager.RMApp;
import resourceManager.RMAppEvent;
import resourceManager.RMAppEventType;
import resourceManager.RMAppImpl;
import resourceManager.RMContext;
import resourceManager.scheduler.ApplicationId;

/**
 * This class manages the list of applications for the resource manager.
 */
public class RMAppManager // implements
							// EventHandler<RMAppManagerEvent>,Recoverable
{

	private static final Logger LOG = LoggerFactory
			.getLogger(RMAppManager.class);

	// private int completedAppsMax =
	// YarnConfiguration.DEFAULT_RM_MAX_COMPLETED_APPLICATIONS;
	// private int globalMaxAppAttempts;
	// private LinkedList<ApplicationId> completedApps = new
	// LinkedList<ApplicationId>();

	private final RMContext rmContext;

	// private final ApplicationMasterService masterService;
	// private final YarnScheduler scheduler;
	// private final ApplicationACLsManager applicationACLsManager;
	// private Configuration conf;

	public RMAppManager(RMContext context
	// YarnScheduler scheduler,
	// ApplicationMasterService masterService,
	// ApplicationACLsManager applicationACLsManager,
	// Configuration conf
	) {
		this.rmContext = context;
		// this.scheduler = scheduler;
		// this.masterService = masterService;
		// this.applicationACLsManager = applicationACLsManager;
		// this.conf = conf;
		// setCompletedAppsMax(conf.getInt(
		// YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS,
		// YarnConfiguration.DEFAULT_RM_MAX_COMPLETED_APPLICATIONS));
		// globalMaxAppAttempts =
		// conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
		// YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
	}

	

	@SuppressWarnings("unchecked")
	public void submitApplication(
			ApplicationSubmissionContext submissionContext, long submitTime,
			boolean isRecovered, String user) throws Exception {
		System.out.println("in RMAppManager:submitApplication() ");
		ApplicationId applicationId = submissionContext.getApplicationId();
		// Create RMApp
		RMApp application = new RMAppImpl(applicationId, rmContext,
		// this.conf,
				submissionContext.getApplicationName(), user,
				submissionContext.getQueue(), submissionContext,
				// this.scheduler,
				// this.masterService,
				submitTime
		// submissionContext.getApplicationType()
		);

		if (rmContext.getRMApps().putIfAbsent(applicationId, application) != null) {
			String message = "Application with id " + applicationId
					+ " is already present! Cannot add a duplicate!";
			LOG.warn(message);
			throw new Exception(message);
		}


		// All done, start the RMApp
		this.rmContext.getDispatcher().register(RMAppEventType.class,
				application);
		this.rmContext
				.getDispatcher()
				.getEventHandler()
				.handle(new RMAppEvent(applicationId,
						isRecovered ? RMAppEventType.RECOVER
								: RMAppEventType.START));
	}

	
}
