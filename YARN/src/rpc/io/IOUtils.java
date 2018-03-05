package rpc.io;

import java.io.IOException;

public class IOUtils {
	  /**
	   * Close the Closeable objects and <b>ignore</b> any {@link IOException} or 
	   * null pointers. Must only be used for cleanup in exception handlers.
	   * @param log the log to record problems to at debug level. Can be null.
	   * @param closeables the objects to close
	   */
	  public static void cleanup( java.io.Closeable... closeables) {
	    for(java.io.Closeable c : closeables) {
	      if (c != null) {
	        try {
	          c.close();
	        } catch(IOException e) {
	        	e.printStackTrace();
	        }
	      }
	    }
	  }

	  /**
	   * Closes the stream ignoring {@link IOException}.
	   * Must only be called in cleaning up from exception handlers.
	   * @param stream the Stream to close
	   */
	  public static void closeStream( java.io.Closeable stream ) {
	    cleanup(stream);
	  }
}
