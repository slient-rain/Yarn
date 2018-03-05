package nodeManager.fs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import nodeManager.shell.Shell.ShellCommandExecutor;

public class Permission {
	public static void chmod777Permission(String filePath) {
		try {
			List<String> command = new ArrayList<String>();
			File file=new File(filePath);
			if(file.isFile()){
				command.addAll(Arrays.asList(
						"chmod","777",filePath
						));
			}
			else if(file.isDirectory()) {
				command.addAll(Arrays.asList(
						"chmod","777",filePath ,"-R"
						));
			}else{
				System.err.println("chmod777Permission.filePath is illegal");
			}
			String[] commandArray = command.toArray(new String[command.size()]);
			ShellCommandExecutor shExec = new ShellCommandExecutor(commandArray); 	
			shExec.execute();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
