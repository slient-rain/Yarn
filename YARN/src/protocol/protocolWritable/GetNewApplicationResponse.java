package protocol.protocolWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.acl.Owner;

import resourceManager.scheduler.ApplicationId;
import resourceManager.scheduler.Resource;
import rpc.io.ObjectWritable;
import rpc.io.Writable;

public class GetNewApplicationResponse implements Writable{
	 ApplicationId applicationId ;
     Resource maximumResourceCapability;
	public GetNewApplicationResponse() {
		super();
		applicationId=new ApplicationId();
		maximumResourceCapability=new Resource();
		
		// TODO Auto-generated constructor stub
	}
	public GetNewApplicationResponse(ApplicationId applicationId,
			Resource maximumResourceCapability) {
		super();
		this.applicationId = applicationId;
		this.maximumResourceCapability = maximumResourceCapability;
	}
	public ApplicationId getApplicationId() {
		return applicationId;
	}
	public void setApplicationId(ApplicationId applicationId) {
		this.applicationId = applicationId;
	}
	public Resource getMaximumResourceCapability() {
		return maximumResourceCapability;
	}
	public void setMaximumResourceCapability(Resource maximumResourceCapability) {
		this.maximumResourceCapability = maximumResourceCapability;
	}
	@Override
	public String toString() {
		return "GetNewApplicationResponse [applicationId=" + applicationId
				+ ", maximumResourceCapability=" + maximumResourceCapability
				+ "]";
	}
	@Override
	public void write(DataOutput out) throws IOException {
		applicationId.write(out);
		maximumResourceCapability.write(out);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		applicationId.readFields(in);
		maximumResourceCapability.readFields(in);
	}
}
