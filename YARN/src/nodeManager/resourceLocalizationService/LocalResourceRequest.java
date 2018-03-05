
package nodeManager.resourceLocalizationService;



import protocol.protocolWritable.ApplicationSubmissionContext;
import protocol.protocolWritable.ApplicationSubmissionContext.LocalResource;
import protocol.protocolWritable.ApplicationSubmissionContext.URL;


public  class LocalResourceRequest {

	URL resource;
	int size;
	String type;
	int timestamp;
	
	
	public LocalResourceRequest() {
		super();
		resource=(new ApplicationSubmissionContext()).new URL();
	}
	public LocalResourceRequest(LocalResource localResource) {
		super();
		this.resource = localResource.getResource();
		this.size = localResource.getSize();
		this.type = localResource.getType();
		this.timestamp = localResource.getTimestamp();
	}
	public URL getResource() {
		return resource;
	}
	public void setResource(URL resource) {
		this.resource = resource;
	}
	public int getSize() {
		return size;
	}
	public void setSize(int size) {
		this.size = size;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public int getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(int timestamp) {
		this.timestamp = timestamp;
	}
	 public LocalResource getLocalResource(){
		 return (new ApplicationSubmissionContext()).new LocalResource(resource, size, type, timestamp);
	 }
	
	
}
