package resourceManager.applicationMasterLauncher.test;

import resourceManager.applicationMasterLauncher.AMLauncher;

public class Main {
	public static void main(String[] args) {
		new Thread(new AMLauncher()).start();
	}
}
