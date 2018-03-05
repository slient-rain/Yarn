package dispatcher.test;

import dispatcher.core.AbstractEvent;

public class JobEvent extends AbstractEvent<JobEventType> {
	private String jobID;
	public JobEvent(String jobID, JobEventType type) {
		super(type);
		this.jobID = jobID;
	}
	public String getJobId() {
		return jobID;
	}
}