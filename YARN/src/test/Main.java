package test;

import resourceManager.scheduler.ApplicationAttemptId;
import resourceManager.scheduler.ApplicationId;

public class Main {
	public static void main(String[] args) {
		//System.out.println(Main.class.getName().toString());
		ApplicationAttemptId id=new ApplicationAttemptId(new ApplicationId(1, 2), 3);
		System.out.println(id.toString());
	}
}
