/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nodeManager.resourceLocalizationService;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import protocol.protocolWritable.ApplicationSubmissionContext.LocalResource;
import nodeManager.container.ContainerEvent;
import nodeManager.container.ContainerEventType;
import nodeManager.container.ContainerImpl;
import nodeManager.fs.FileContext;
import nodeManager.fs.Path;
import dispatcher.core.Dispatcher;
import dispatcher.core.EventHandler;
import resourceManager.scheduler.ContainerId;
import service.AbstractService;
import service.CompositeService;
import util.PropertiesFile;

public class ResourceLocalizationService extends AbstractService
// extends CompositeService
		implements EventHandler<LocalizerEvent>
// implements EventHandler<LocalizationEvent>, LocalizationProtocol
{
	private static final Logger LOG = LoggerFactory
			.getLogger(ResourceLocalizationService.class);
	public static String LOCAL_DIR;
	public static final String FILECACHE = "filecache";

	protected final Dispatcher dispatcher;

	LocalizerTracker localizerTracker;

	@Override
	public void handle(LocalizerEvent event) {
		localizerTracker.handle(event);
	}

	public ResourceLocalizationService(Dispatcher dispatcher
	// ContainerExecutor exec,
	// DeletionService delService,
	// LocalDirsHandlerService dirsHandler
	) {

		super(ResourceLocalizationService.class.getName());
		localizerTracker = new LocalizerTracker();
		// this.exec = exec;
		this.dispatcher = dispatcher;

		PropertiesFile pf = new PropertiesFile("config.properties");
		LOCAL_DIR = pf.get("local_dir");
	}

	@Override
	public void serviceInit() throws Exception {
		localizerTracker.init();
		super.serviceInit();
	}

	@Override
	public void serviceStart() throws Exception {

		localizerTracker.start();
		super.serviceStart();
	}

	@Override
	public void serviceStop() throws Exception {
		// if (server != null) {
		// server.stop();
		// }
		// cacheCleanup.shutdown();
		localizerTracker.stop();
		super.serviceStop();
	}

	/**
	 * Sub-component handling the spawning of {@link ContainerLocalizer}s
	 */
	class LocalizerTracker extends AbstractService implements
			EventHandler<LocalizerEvent> {

		private final PublicLocalizer publicLocalizer;

		// private final Map<String,LocalizerRunner> privLocalizers;

		LocalizerTracker() {
			super("");
			this.publicLocalizer = new PublicLocalizer();
		}

		@Override
		public synchronized void serviceStart() throws Exception {
			publicLocalizer.start();
			super.serviceStart();
		}

		@Override
		public void serviceStop() throws Exception {
			// for (LocalizerRunner localizer : privLocalizers.values()) {
			// localizer.interrupt();
			// }
			publicLocalizer.interrupt();
			super.serviceStop();
		}

		@Override
		public void handle(LocalizerEvent event) {
			String locId = event.getLocalizerId();
			String containerID = event.getLocalizerId();
			LOG.debug("Processing " + containerID + " of type "
					+ event.getType());
			switch (event.getType()) {
			case REQUEST_RESOURCE_LOCALIZATION:
				// 0) find running localizer or start new thread
				LocalizerResourceRequestEvent req = (LocalizerResourceRequestEvent) event;
				switch (req.getVisibility()) {
				case PUBLIC:
					publicLocalizer.addResource(req);
					break;
				case PRIVATE:
				case APPLICATION:
					// synchronized (privLocalizers) {
					// LocalizerRunner localizer = privLocalizers.get(locId);
					// if (null == localizer) {
					// LOG.info("Created localizer for " + locId);
					// localizer = new LocalizerRunner(req.getContext(), locId);
					// privLocalizers.put(locId, localizer);
					// localizer.start();
					// }
					// // 1) propagate event
					// localizer.addResource(req);
					// }
					break;
				}
				break;
			}
		}
	}

	class PublicLocalizer extends Thread {

		final FileContext lfs;
		final ExecutorService threadPool;
		final CompletionService<Path> queue;
		final int nThreads = 10;
		// Its shared between public localizer and dispatcher thread.
		final Map<Future<Path>, LocalizerResourceRequestEvent> pending;

		PublicLocalizer() {
			super("Public Localizer");
			this.lfs = new FileContext();
			this.pending = new ConcurrentHashMap<Future<Path>, LocalizerResourceRequestEvent>();
			this.threadPool = Executors.newFixedThreadPool(nThreads);
			this.queue = new ExecutorCompletionService<Path>(threadPool);
		}

		public void addResource(LocalizerResourceRequestEvent request) {
			String applicationIdStr = request.getContext().getApplicatioId()
					.toString();
			String containerIdStr = request.getContext().getContainerId()
					.toString();

			Path path = new Path(LOCAL_DIR + (Path.SEPARATOR) + FILECACHE
					+ (Path.SEPARATOR) + applicationIdStr + (Path.SEPARATOR)
					+ containerIdStr);
			LOG.debug("util check: addResource() request:"
					+ request.toString());
			LOG.debug("util check: addResource().mkdir.path:"
							+ path.toString());
			lfs.mkdir(path.toString());
			Map<LocalResourceVisibility, Collection<LocalResourceRequest>> resources = request
					.getResource();
			for (Map.Entry<LocalResourceVisibility, Collection<LocalResourceRequest>> entry : resources
					.entrySet()) {
				List<LocalResourceRequest> list = (List) entry.getValue();
				for (int i = 0; i < list.size(); i++) {
					LocalResourceRequest request2 = list.get(i);
					pending.put(queue.submit(new FSDownload(path, request2
							.getLocalResource()
					// lfs, null, conf,
					// path, resource
							)), request);
				}

			}
		}

		@Override
		public void run() {
			// try {
			// TODO shutdown, better error handling esp. DU
			while (!Thread.currentThread().isInterrupted()) {
				try {
					Future<Path> completed = queue.take();
					LocalizerResourceRequestEvent assoc = pending
							.remove(completed);
					try {
						Path local = completed.get();
						LOG.debug("util check: run():下载到的本地目录"
										+ local);
						dispatcher.getEventHandler().handle(
								new ContainerEvent(assoc.getContext()
										.getContainerId(),
										ContainerEventType.RESOURCE_LOCALIZED));

					} catch (InterruptedException e) {
						return;
					} catch (ExecutionException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					// } catch(Throwable t) {
					// LOG.fatal("Error: Shutting down", t);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					threadPool.shutdownNow();
				}
				// }

			}
		}
	}

}
