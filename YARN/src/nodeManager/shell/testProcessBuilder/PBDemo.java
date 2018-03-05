package nodeManager.shell.testProcessBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;

class PBDemo { 
	public static void main(String args[]) { 
		try { 	
			//System.setOut(new PrintStream("src\\testfile.txt"));
			ProcessBuilder proc = new ProcessBuilder("java" ,"-jar","f:\\dispatcher.jar");    
			Process process=proc.start(); 
			final BufferedReader errReader = 
					new BufferedReader(new InputStreamReader(process.getErrorStream()));
			BufferedReader inReader = 
					new BufferedReader(new InputStreamReader(process.getInputStream()));
			System.out.println(proc.command());

			/**
			 * get error message of .jar
			 */
			final StringBuffer errMsg = new StringBuffer();
			Thread errThread = new Thread() {
				@Override
				public void run() {
					try {
						String line = errReader.readLine();
						while((line != null) && !isInterrupted()) {
							errMsg.append(line);
							errMsg.append("\n");
							line = errReader.readLine();
						}
						System.out.println("error message in jar: "+errMsg.toString());
					} catch(IOException ioe) {
						System.out.println("Error reading the error stream");
					}
				}
			};
			errThread.start();
			final StringBuffer outMsg = new StringBuffer();	
			parseExecResult(inReader,outMsg); // parse the output
			String line = inReader.readLine();
			while(line != null) { 
				//						outMsg.append(line);
				//						outMsg.append("|**|");
				line = inReader.readLine();

			}					
			System.out.println("output message in jar:"+outMsg.toString());



		} catch (Exception e) { 
			System.out.println("Error executing notepad."); 
		} 

	} 

	static void parseExecResult(BufferedReader lines,StringBuffer output) throws IOException {
		char[] buf = new char[512];
		int nRead;
		while ( (nRead = lines.read(buf, 0, buf.length)) > 0 ) {
			output.append(buf, 0, nRead);
		}
	}
}
