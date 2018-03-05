package dispatcher.test;

public class Main {
	public static void main(String[] args) throws Exception {
		String jobID = "job_20131215_12";
		SimpleMRAppMaster appMaster = new SimpleMRAppMaster("Simple MRAppMaster", jobID, 5);
		//YarnConfiguration conf = new YarnConfiguration(new Configuration());
		appMaster.init();
		appMaster.start();
		appMaster.getDispatcher().getEventHandler().handle(new JobEvent(jobID,
				JobEventType.JOB_KILL));
		appMaster.getDispatcher().getEventHandler().handle(new JobEvent(jobID,
				JobEventType.JOB_INIT));
		Thread.sleep(10000);
		appMaster.stop();
	}
}
