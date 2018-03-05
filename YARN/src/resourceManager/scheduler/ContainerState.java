package resourceManager.scheduler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import rpc.io.Writable;

/**
 * <p>State of a <code>Container</code>.</p>
 */

public enum ContainerState  {
  /** New container */
  NEW, 
  
  /** Running container */
  RUNNING, 
  
  /** Completed container */
  COMPLETE;


}