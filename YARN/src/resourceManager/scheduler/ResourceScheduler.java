package resourceManager.scheduler;

/**
 * This interface is the one implemented by the schedulers. It mainly extends 
 * {@link YarnScheduler}. 
 *
 */
public interface ResourceScheduler extends YarnScheduler{
  /**
   * Re-initialize the <code>ResourceScheduler</code>.
   * @param conf configuration
   * @throws IOException
   */
  //void reinitialize(Configuration conf, RMContext rmContext) throws IOException;
}
