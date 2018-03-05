package dispatcher.event;

import dispatcher.core.AbstractEvent;


public class ApplicationEvent extends AbstractEvent<ApplicationEventType> {
	private String applicationId;
	public ApplicationEvent(String applicationId, ApplicationEventType type) {
		super(type);
		this.applicationId = applicationId;
	}
	public String getApplicationId() {
		return applicationId;
	}
}