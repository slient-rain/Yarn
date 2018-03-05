package resourceManager.scheduler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.NumberFormat;

import rpc.io.Writable;

/**
 * ApplicationAttemptId+累加计数器containerId
 * <p><code>ContainerId</code> represents a globally unique identifier
 * for a {@link Container} in the cluster.</p>
 */

public  class ContainerId implements Comparable<ContainerId>,Writable{
	ApplicationAttemptId appAttemptId;
	int containerId;
	public ContainerId(){
		appAttemptId=new ApplicationAttemptId();
	}
	public ContainerId(ApplicationAttemptId appAttemptId,
			int containerId){
		this.appAttemptId=appAttemptId;
		this.containerId=containerId;
	}
	
	public static ContainerId newInstance(ApplicationAttemptId appAttemptId,
			int containerId) {		
		return new ContainerId(appAttemptId,containerId);
	}

	public ApplicationAttemptId getApplicationAttemptId() {
		return appAttemptId;
	}

	public void setApplicationAttemptId(ApplicationAttemptId appAttemptId) {
		this.appAttemptId = appAttemptId;
	}

	public int getId() {
		return containerId;
	}

	public void setId(int containerId) {
		this.containerId = containerId;
	}

	// TODO: fail the app submission if attempts are more than 10 or something
	private static final ThreadLocal<NumberFormat> appAttemptIdFormat =
			new ThreadLocal<NumberFormat>() {
		@Override
		public NumberFormat initialValue() {
			NumberFormat fmt = NumberFormat.getInstance();
			fmt.setGroupingUsed(false);
			fmt.setMinimumIntegerDigits(2);
			return fmt;
		}
	};
	// TODO: Why thread local?
	// ^ NumberFormat instances are not threadsafe
	private static final ThreadLocal<NumberFormat> containerIdFormat =
			new ThreadLocal<NumberFormat>() {
		@Override
		public NumberFormat initialValue() {
			NumberFormat fmt = NumberFormat.getInstance();
			fmt.setGroupingUsed(false);
			fmt.setMinimumIntegerDigits(6);
			return fmt;
		}
	};

	@Override
	public int hashCode() {
		// Generated by eclipse.
		final int prime = 435569;
		int result = 7507;
		result = prime * result + getId();
		result = prime * result + getApplicationAttemptId().hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ContainerId other = (ContainerId) obj;
		if (!this.getApplicationAttemptId().equals(other.getApplicationAttemptId()))
			return false;
		if (this.getId() != other.getId())
			return false;
		return true;
	}

	@Override
	public int compareTo(ContainerId other) {
		if (this.getApplicationAttemptId().compareTo(
				other.getApplicationAttemptId()) == 0) {
			return this.getId() - other.getId();
		} else {
			return this.getApplicationAttemptId().compareTo(
					other.getApplicationAttemptId());
		}

	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("container_");
		ApplicationId appId = getApplicationAttemptId().getApplicationId();
		sb.append(appId.getClusterTimestamp()).append("_");
		sb.append(ApplicationId.appIdFormat.get().format(appId.getId()))
		.append("_");
		sb.append(
				appAttemptIdFormat.get().format(
						getApplicationAttemptId().getAttemptId())).append("_");
		sb.append(containerIdFormat.get().format(getId()));
		return sb.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		appAttemptId.write(out);
		out.writeInt(containerId);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		appAttemptId.readFields(in);
		containerId=in.readInt();
		
	}
}
