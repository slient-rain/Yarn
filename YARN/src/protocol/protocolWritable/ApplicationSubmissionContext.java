package protocol.protocolWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sun.org.apache.bcel.internal.generic.NEW;

import resourceManager.scheduler.ApplicationId;
import resourceManager.scheduler.Priority;
import resourceManager.scheduler.Resource;
import rpc.io.UTF8;
import rpc.io.Writable;

public class ApplicationSubmissionContext implements Writable {
	ApplicationId applicationId ;
	Map<String, LocalResource> localResouces;
	Resource resource;
	String applicationName;
	Priority priority;
	List<String> command;
	String queue;
	String user;
	public ApplicationSubmissionContext() {
		super();
		applicationId=new ApplicationId();
		localResouces=new HashMap<String, ApplicationSubmissionContext.LocalResource>();
		resource=new Resource();
		priority=new Priority();
		command=new ArrayList<String>();
	}
	
	public ApplicationId getApplicationId() {
		return applicationId;
	}

	public void setApplicationId(ApplicationId applicationId) {
		this.applicationId = applicationId;
	}

	public Map<String, LocalResource> getLocalResouces() {
		return localResouces;
	}

	public void setLocalResouces(Map<String, LocalResource> localResouces) {
		this.localResouces = localResouces;
	}

	public Resource getResource() {
		return resource;
	}

	public void setResource(Resource resource) {
		this.resource = resource;
	}

	public String getApplicationName() {
		return applicationName;
	}

	public void setApplicationName(String applicationName) {
		this.applicationName = applicationName;
	}

	public Priority getPriority() {
		return priority;
	}

	public void setPriority(Priority priority) {
		this.priority = priority;
	}

	public List<String> getCommand() {
		return command;
	}

	public void setCommand(List<String> command) {
		this.command = command;
	}

	public String getQueue() {
		return queue;
	}

	public void setQueue(String queue) {
		this.queue = queue;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public ApplicationSubmissionContext(ApplicationId applicationId,
			Map<String, LocalResource> localResouces, Resource resource,
			String applicationName, Priority priority, List<String> command,
			String queue, String user) {
		super();
		this.applicationId = applicationId;
		this.localResouces = localResouces;
		this.resource = resource;
		this.applicationName = applicationName;
		this.priority = priority;
		this.command = command;
		this.queue = queue;
		this.user = user;
	}
	

	@Override
	public String toString() {
		return "ApplicationSubmissionContext [applicationId=" + applicationId
				+ ", localResouces=" + localResouces + ", resource=" + resource
				+ ", applicationName=" + applicationName + ", priority="
				+ priority + ", command=" + command + ", queue=" + queue
				+ ", user=" + user + "]";
	}

	@Override
	public void write(DataOutput out) throws IOException {
		applicationId.write(out);
		out.writeInt(localResouces.size());
		for (Map.Entry<String, LocalResource> e : localResouces.entrySet()) {
			UTF8.writeString(out, e.getKey());
			e.getValue().write(out);
		}
		resource.write(out);
		UTF8.writeString(out, applicationName);
		priority.write(out);
		out.writeInt(command.size());
		for(int i=0; i<command.size(); i++){
			UTF8.writeString(out, command.get(i));
		}
		UTF8.writeString(out, queue);
		UTF8.writeString(out, user);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		applicationId.readFields(in);
		int n =in.readInt();
		for(int i=0;i<n ;i++){
			String key=UTF8.readString(in);
			LocalResource value=new LocalResource();
			value.readFields(in);
			localResouces.put(key,value);
		}
		resource.readFields(in);
		applicationName=UTF8.readString(in);
		priority.readFields(in);
		n =in.readInt();
		List<String> list=new ArrayList<String>();
		for(int i=0; i<n; i++){
			String temp=UTF8.readString(in);
			command.add(temp);
		}	
		queue=UTF8.readString(in);
		user=UTF8.readString(in);

	}

	public class LocalResource implements Writable{
		URL resource;
		int size;
		String type;
		int timestamp;
		
		
		public LocalResource() {
			super();
			resource=new URL();
		}
		public LocalResource(URL resource, int size, String type, int timestamp) {
			super();
			this.resource = resource;
			this.size = size;
			this.type = type;
			this.timestamp = timestamp;
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
		@Override
		public String toString() {
			return "LocalResource [resource=" + resource + ", size=" + size
					+ ", type=" + type + ", timestamp=" + timestamp + "]";
		}
		@Override
		public void write(DataOutput out) throws IOException {
			resource.write(out);
			out.writeInt(size);
			UTF8.writeString(out, type);
			out.writeInt(timestamp);
			
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			resource.readFields(in);
			size=in.readInt();
			type=UTF8.readString(in);
			timestamp=in.readInt();
			
		}
	}

	public class URL implements Writable{
		String host;
		int port;
		String file;
		
		
		public URL() {
			super();
			// TODO Auto-generated constructor stub
		}
		public URL(String host, int port, String file) {
			super();
			this.host = host;
			this.port = port;
			this.file = file;
		}
		public String getHost() {
			return host;
		}
		public void setHost(String host) {
			this.host = host;
		}
		public int getPort() {
			return port;
		}
		public void setPort(int port) {
			this.port = port;
		}
		public String getFile() {
			return file;
		}
		public void setFile(String file) {
			this.file = file;
		}
		@Override
		public String toString() {
			return "URL [host=" + host + ", port=" + port + ", file=" + file
					+ "]";
		}
		@Override
		public void write(DataOutput out) throws IOException {			
			UTF8.writeString(out, host);
			out.writeInt(port);
			UTF8.writeString(out, file);
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			host=UTF8.readString(in);
			port=in.readInt();
			file=UTF8.readString(in);
			
		}
	}
	

}
