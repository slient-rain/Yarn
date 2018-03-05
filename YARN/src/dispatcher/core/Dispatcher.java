package dispatcher.core;
/**
 * Event Dispatcher interface. It dispatches events to registered 
 * event handlers based on event types.
 * 
 */
public interface Dispatcher  {
	EventHandler getEventHandler();
	void register(Class<? extends Enum> eventType, EventHandler handler);
}
