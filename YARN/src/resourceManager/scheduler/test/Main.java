package resourceManager.scheduler.test;

import resourceManager.scheduler.NodeState;

public class Main {
	public static void main(String[] args) {
		System.out.println(NodeState.NEW.name());
		System.out.println(NodeState.valueOf(NodeState.NEW.name()));
	}
		
	
}
