package recoverable.stateStore;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import dispatcher.core.Dispatcher;



import protocol.protocolWritable.ApplicationSubmissionContext;
import resourceManager.RMAppEvent;
import resourceManager.RMAppEventType;
import resourceManager.RMAppImpl;
import resourceManager.scheduler.ApplicationId;

public class RMStateStore {
	RMState state = new RMState();
	Dispatcher dispatcher;
	
	public Dispatcher getDispatcher() {
		return dispatcher;
	}

	public void setDispatcher(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
	}

	public void storeApplication(RMAppImpl app){
		System.out.println("in RMState : storeApplication()");
		
		ApplicationSubmissionContext context = app
                .getApplicationSubmissionContext();
		assert context instanceof ApplicationSubmissionContext;
	    ApplicationState appState = new ApplicationState(
	        app.getSubmitTime(), context, app.getUser());

	    storeApplicationState(app.getApplicationId().toString(), appState);
	    dispatcher.getEventHandler().handle(new RMAppEvent(app.getApplicationId(), RMAppEventType.APP_SAVED));
	}
	
	public void storeApplicationState(String appId, ApplicationState appState){
		 state.appState.put(appState.getAppId(), appState);
	}
	
	public RMState getRMState(){
		return state;
	}
	/**
	   * State of an application application
	   */
	  public static class ApplicationState {
	    final ApplicationSubmissionContext context;
	    final long submitTime;
	    final String user;
//	    Map<ApplicationAttemptId, ApplicationAttemptState> attempts =
//	                  new HashMap<ApplicationAttemptId, ApplicationAttemptState>();
	    
	    ApplicationState(long submitTime, ApplicationSubmissionContext context,
	        String user) {
	      this.submitTime = submitTime;
	      this.context = context;
	      this.user = user;
	    }

	    @Override
		public String toString() {
			return "ApplicationState [context=" + context.toString() + ", submitTime="
					+ submitTime + ", user=" + user + "]";
		}

		public ApplicationId getAppId() {
	      return context.getApplicationId();
	    }
	    public long getSubmitTime() {
	      return submitTime;
	    }
//	    public int getAttemptCount() {
//	      return attempts.size();
//	    }
	    public ApplicationSubmissionContext getApplicationSubmissionContext() {
	      return context;
	    }
//	    public ApplicationAttemptState getAttempt(ApplicationAttemptId attemptId) {
//	      return attempts.get(attemptId);
//	    }
	    public String getUser() {
	      return user;
	    }
	  }
	  
	  /**
	   * 备忘录管理者
	   * @author 无言的雨
	   *
	   */
	  public static class RMState {
	    Map<ApplicationId, ApplicationState> appState =
	        new HashMap<ApplicationId, ApplicationState>();

//	    RMDTSecretManagerState rmSecretManagerState = new RMDTSecretManagerState();

	    public Map<ApplicationId, ApplicationState> getApplicationState() {
	      return appState;
	    }

//	    public RMDTSecretManagerState getRMDTSecretManagerState() {
//	      return rmSecretManagerState;
//	    }
	  }
}
