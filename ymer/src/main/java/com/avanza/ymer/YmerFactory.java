/*
 * Copyright 2015 Avanza Bank AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.avanza.ymer;

import com.avanza.ymer.plugin.Plugin;
import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.mongodb.ReadPreference;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
/**
 * @author Elias Lindholm (elilin)
 *
 */
public final class YmerFactory {

	private MirrorExceptionListener exceptionListener = new MirrorExceptionListener() {
		@Override
		public void onMirrorException(Exception e, MirrorOperation failedOperation, Object[] failedObjects) {}
	};
	private ReadPreference readPreference = ReadPreference.primary();
	private boolean exportExceptionHandleMBean = true;
	private Set<Plugin> plugins = Collections.emptySet();
	private int numParallelCollections = 1;

	private final MirroredObjects mirroredObjects;
	private final MongoConverter mongoConverter;
	private final MongoDbFactory mongoDbFactory;


	public YmerFactory(MongoDbFactory mongoDbFactory,
					   MongoConverter mongoConverter,
					   Collection<MirroredObjectDefinition<?>> definitions) {
		this.mongoDbFactory = mongoDbFactory;
		this.mongoConverter = mongoConverter;
		this.mirroredObjects = new MirroredObjects(definitions.stream(), MirroredObjectDefinitionsOverride.fromSystemProperties());
	}

	/**
	 * Defines whether an ExceptionHandlerMBean should be exported. The ExceptionHandlerMBean allows setting the SpaceSynchronizationEndpoint
	 * in a state where a bulk of operations is discarded if a failure occurs during synchronization. The default behavior is to keep a failed bulk
	 * operation first in the queue and wait for a defined interval before running a new attempt to synchronize the bulk. This blocks all
	 * subsequent synchronization operations until the bulk succeeds.
	 *
	 * Default is "true"
	 *
	 * @param exportExceptionHandleMBean
	 */
	public void setExportExceptionHandlerMBean(boolean exportExceptionHandleMBean) {
		this.exportExceptionHandleMBean = exportExceptionHandleMBean;
	}

	/**
	 * Sets a MirrorExceptionListener (optional). <p>
	 *
	 * @param exceptionListener
	 */
	public void setExceptionListener(MirrorExceptionListener exceptionListener) {
		this.exceptionListener = exceptionListener;
	}

	public void setPlugins(Set<Plugin> plugins) {
		this.plugins = plugins;
	}

	public void setNumParallelCollections(int numParallelCollections) {
		if (numParallelCollections < 1) {
			throw new IllegalArgumentException("numParallelCollections must be a positive integer, was numParallelCollections=" + numParallelCollections + "!");
		}
		this.numParallelCollections = numParallelCollections;
	}

	/**
	 * Sets the read preference for queries against all document collections.
	 * Use {@link ReadPreference#secondaryPreferred} or
	 * {@link ReadPreference#primaryPreferred} to enable reads from mongo
	 * secondaries. Default is {@link ReadPreference#primary}.
	 */
	public YmerFactory withReadPreference(ReadPreference readPreference) {
		this.readPreference = Objects.requireNonNull(readPreference);
		return this;
	}

	public SpaceDataSource createSpaceDataSource() {
		return new YmerSpaceDataSource(createSpaceMirrorContext());
	}

	public SpaceSynchronizationEndpoint createSpaceSynchronizationEndpoint() {
		YmerSpaceSynchronizationEndpoint ymerSpaceSynchronizationEndpoint = new YmerSpaceSynchronizationEndpoint(createSpaceMirrorContext());
		if (this.exportExceptionHandleMBean) {
			ymerSpaceSynchronizationEndpoint.registerExceptionHandlerMBean();
		}
		return ymerSpaceSynchronizationEndpoint;
	}

	private SpaceMirrorContext createSpaceMirrorContext() {
		DocumentDb documentDb = DocumentDb.mongoDb(this.mongoDbFactory.getDb(), readPreference);
		DocumentConverter documentConverter = DocumentConverter.mongoConverter(mongoConverter);
		// Set the event publisher to null to avoid deadlocks when loading data in parallel
		if (mongoConverter.getMappingContext() instanceof ApplicationEventPublisherAware) {
			((ApplicationEventPublisherAware)mongoConverter.getMappingContext()).setApplicationEventPublisher(null);
		}
		SpaceMirrorContext mirrorContext = new SpaceMirrorContext(mirroredObjects, documentConverter, documentDb, exceptionListener, new Plugins(plugins), numParallelCollections);
		return mirrorContext;
	}

}
