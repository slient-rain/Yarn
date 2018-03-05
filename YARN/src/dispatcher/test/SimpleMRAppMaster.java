package dispatcher.test;

import service.AbstractService;
import service.CompositeService;
import service.Service;
import dispatcher.core.AsyncDispatcher;
import dispatcher.core.Dispatcher;
import dispatcher.core.EventHandler;

@SuppressWarnings("unchecked")
public class SimpleMRAppMaster extends CompositeService {
	private Dispatcher dispatcher; // 中央异步调度器
	private String jobID;
	private int taskNumber; // 该作业包含的任务数目
	private String[] taskIDs; // 该作业内部包含的所有任务
	public SimpleMRAppMaster(String name, String jobID, int taskNumber) {
		super(name);
		this.jobID = jobID;
		this.taskNumber = taskNumber;
		taskIDs = new String[taskNumber];
		for(int i = 0; i < taskNumber; i++) {
			taskIDs[i] = new String(jobID + "_task_" + i);
		}
	}
	public void serviceInit() throws Exception {
		dispatcher = new AsyncDispatcher();// 定义一个中央异步调度器
		//dispatcher.serviceStart();
		// 分别注册 Job 和 Task 事件调度器
		dispatcher.register(JobEventType.class, new JobEventDispatcher());
		dispatcher.register(TaskEventType.class, new TaskEventDispatcher());
		addService((Service) dispatcher);
		super.serviceInit();
	}
	public Dispatcher getDispatcher() {
		return dispatcher;
	}
	private class JobEventDispatcher implements EventHandler<JobEvent> {
		@Override
		public void handle(JobEvent event) {
			if(event.getType() == JobEventType.JOB_KILL) {
				System.out.println("Receive JOB_KILL event, killing all the tasks");
				for(int i = 0; i < taskNumber; i++) {
					dispatcher.getEventHandler().handle(new TaskEvent(taskIDs[i],
							TaskEventType.T_KILL));
				}
			} else if(event.getType() == JobEventType.JOB_INIT) {
				System.out.println("Receive JOB_INIT event, scheduling tasks");
				for(int i = 0; i < taskNumber; i++) {
					dispatcher.getEventHandler().handle(new TaskEvent(taskIDs[i],
							TaskEventType.T_SCHEDULE));
				}
			}
		}
	}
	private class TaskEventDispatcher implements EventHandler<TaskEvent> {
		@Override
		public void handle(TaskEvent event) {
			if(event.getType() == TaskEventType.T_KILL) {
				System.out.println("Receive T_KILL event of task " + event.getTaskID());
			} else if(event.getType() == TaskEventType.T_SCHEDULE) {
				System.out.println("Receive T_SCHEDULE event of task " + event.getTaskID());
			}
		}
	}
}