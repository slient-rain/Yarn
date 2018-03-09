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

	private final RMContext rmContext;
	public RMAppManager(RMContext context
	) {
		this.rmContext = context;
	}

	

	@SuppressWarnings("unchecked")
	public void submitApplication(
			ApplicationSubmissionContext submissionContext, long submitTime,
			boolean isRecovered, String user) throws Exception {
		ApplicationId applicationId = submissionContext.getApplicationId();
		// Create RMApp
		RMApp application = new RMAppImpl(applicationId, rmContext,
				submissionContext.getApplicationName(), user,
				submissionContext.getQueue(), submissionContext,
				submitTime
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
