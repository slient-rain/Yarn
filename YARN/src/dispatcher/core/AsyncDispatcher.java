package dispatcher.core;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import service.AbstractService;



public class AsyncDispatcher extends AbstractService implements Dispatcher{
	private final BlockingQueue<Event> eventQueue;
	private Thread eventHandlingThread;
	protected final Map<Class<? extends Enum>, EventHandler> eventDispatchers;

	private volatile boolean stopped = false;
	private static final Logger LOG = LoggerFactory.getLogger(AsyncDispatcher.class);

	public AsyncDispatcher() {
		super("Dispatcher");
		this.eventQueue=new LinkedBlockingQueue<Event>();
		this.eventDispatchers = new ConcurrentHashMap<Class<? extends Enum>, EventHandler>();
	}
	@Override
	protected void serviceInit() throws Exception {
		super.serviceInit();
	}
	@Override
	public void serviceStart() throws Exception  {
		//start all the components
		super.serviceStart();
		eventHandlingThread = new Thread(createThread());
		eventHandlingThread.setName("AsyncDispatcher event handler");
		eventHandlingThread.start();
	}

	@Override
	protected void serviceStop() throws Exception {
		stopped = true;
		if (eventHandlingThread != null) {
			eventHandlingThread.interrupt();
			try {
				eventHandlingThread.join();
			} catch (InterruptedException ie) {
				LOG.warn("Interrupted Exception while stopping", ie);
			}
		}
		// stop all the components
		super.serviceStop();
	}

	Runnable createThread() {
		return new Runnable() {
			@Override
			public void run() {
				while (!stopped && !Thread.currentThread().isInterrupted()) {
					Event event;
					try {
						event = eventQueue.take();
					} catch(InterruptedException ie) {
						if (!stopped) {
							LOG.warn("AsyncDispatcher thread interrupted", ie);
						}
						return;
					}
					if (event != null) {
						dispatch(event);
					}
				}
			}
		};
	}

	@SuppressWarnings("unchecked")
	protected void dispatch(Event event) {
		//all events go thru this loop
		if (LOG.isDebugEnabled()) {
			LOG.debug("Dispatching the event " + event.getClass().getName() + "."+ event.toString());
		}
		Class<? extends Enum> type = event.getType().getDeclaringClass();
		try{
			EventHandler handler = eventDispatchers.get(type);
			if(handler != null) {
				handler.handle(event);
			} else {
				throw new Exception("No handler for registered for " + type);
			}
		}
		catch (Throwable t) {
			//TODO Maybe log the state of the queue
			LOG.error("Error in dispatcher thread",t);
			//	      if (exitOnDispatchException
			//	          && (ShutdownHookManager.get().isShutdownInProgress()) == false) {
			//	        LOG.info("Exiting, bbye..");
			//	        System.exit(-1);
			//	      }
		}
	}

	@Override
	public EventHandler getEventHandler() {
		return new GenericEventHandler();
	}

	@Override
	public void register(Class<? extends Enum> eventType, EventHandler handler) {
		/**
		 * check to see if we have a listener registered 
		 */
		EventHandler<Event> registeredHandler = (EventHandler<Event>)eventDispatchers.get(eventType);
		LOG.info("Registering " + eventType + " for " + handler.getClass());
		if (registeredHandler == null) {
			eventDispatchers.put(eventType, handler);
		} else if (!(registeredHandler instanceof MultiListenerHandler)){
			/* for multiple listeners of an event add the multiple listener handler */
			MultiListenerHandler multiHandler = new MultiListenerHandler();
			multiHandler.addHandler(registeredHandler);
			multiHandler.addHandler(handler);
			eventDispatchers.put(eventType, multiHandler);
		} else {
			/* already a multilistener, just add to it */
			MultiListenerHandler multiHandler = (MultiListenerHandler) registeredHandler;
			multiHandler.addHandler(handler);
		}
	}

	class GenericEventHandler implements EventHandler<Event> {
		/**
		 * all this method does is enqueue all the events onto the queue 
		 */
		public void handle(Event event) {
			int qSize = eventQueue.size();
			if (qSize !=0 && qSize %1000 == 0) {
				if(LOG.isInfoEnabled())
					LOG.info("Size of event-queue is " + qSize);
			}
			int remCapacity = eventQueue.remainingCapacity();
			if (remCapacity < 1000) {
				LOG.warn("Very low remaining capacity in the event-queue: "	+ remCapacity);
			}
			try {
				eventQueue.put(event);
			} catch (InterruptedException e) {
				if (!stopped) {
					LOG.warn("AsyncDispatcher thread interrupted", e);
				}
				//		        throw new YarnRuntimeException(e);
			}
		}
	}

	/**
	 * Multiplexing an event. Sending it to different handlers that
	 * are interested in the event.
	 * @param <T> the type of event these multiple handlers are interested in.
	 */
	static class MultiListenerHandler implements EventHandler<Event> {
		List<EventHandler<Event>> listofHandlers;

		public MultiListenerHandler() {
			listofHandlers = new ArrayList<EventHandler<Event>>();
		}

		@Override
		public void handle(Event event) {
			for (EventHandler<Event> handler: listofHandlers) {
				handler.handle(event);
			}
		}

		void addHandler(EventHandler<Event> handler) {
			listofHandlers.add(handler);
		}

	}
}
