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

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import com.avanza.ymer.plugin.Plugin;
import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoDatabase;

/**
 * @author Elias Lindholm (elilin)
 *
 */
public final class YmerFactory implements ApplicationContextAware {
	private static final Logger LOG = LoggerFactory.getLogger(YmerFactory.class);

	private MirrorExceptionListener exceptionListener = (e, failedOperation, failedObjects) -> {};
	private ReadPreference readPreference = ReadPreference.primary();
	private boolean exportExceptionHandleMBean = true;
	private Set<Plugin> plugins = Collections.emptySet();
	private int numParallelCollections = 1;
	private ReloadableYmerProperties.ReloadablePropertiesBuilder ymerPropertiesBuilder = ReloadableYmerProperties.builder();

	private final MirroredObjects mirroredObjects;
	private final MongoConverter mongoConverter;
	private final Supplier<MongoDatabase> mongoDatabaseSupplier;

	@Nullable
	private ApplicationContext applicationContext;

	public YmerFactory(Supplier<MongoDatabase> mongoDatabaseSupplier,
					   MongoConverter mongoConverter,
					   Collection<MirroredObjectDefinition<?>> definitions) {
		this.mongoDatabaseSupplier = mongoDatabaseSupplier;
		this.mongoConverter = mongoConverter;
		this.mirroredObjects = new MirroredObjects(definitions.stream(), MirroredObjectDefinitionsOverride.fromSystemProperties());
		if (mirroredObjects.getMirroredTypes().isEmpty()) {
			LOG.warn(""
					+ "The list of mirrored object types is empty. This is "
					+ "almost always an error since either the app wants to "
					+ "use Ymer to store or load documents, in which case "
					+ "there should be at least one type of mirrored object, "
					+ "or the app does not wish to store or load documents, in "
					+ "which case it should not instantiate YmerFactory."
			);
		}
	}

	/**
	 * @deprecated please use {@link #YmerFactory(MongoDatabaseFactory,MongoConverter,Collection)}
	 */
	@Deprecated
	public YmerFactory(MongoDbFactory mongoDbFactory,
					   MongoConverter mongoConverter,
					   Collection<MirroredObjectDefinition<?>> definitions) {
		this(mongoDbFactory::getDb, mongoConverter, definitions);
	}

	public YmerFactory(MongoDatabaseFactory mongoDbFactory,
					   MongoConverter mongoConverter,
					   Collection<MirroredObjectDefinition<?>> definitions) {
		this(mongoDbFactory::getMongoDatabase, mongoConverter, definitions);
	}

	@Override
	public void setApplicationContext(@Nonnull ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	/**
	 * Defines whether an ExceptionHandlerMBean should be exported. The ExceptionHandlerMBean allows setting the SpaceSynchronizationEndpoint
	 * in a state where a bulk of operations is discarded if a failure occurs during synchronization. The default behavior is to keep a failed bulk
	 * operation first in the queue and wait for a defined interval before running a new attempt to synchronize the bulk. This blocks all
	 * subsequent synchronization operations until the bulk succeeds.
	 *
	 * Default is "true"
	 *
	 */
	public void setExportExceptionHandlerMBean(boolean exportExceptionHandleMBean) {
		this.exportExceptionHandleMBean = exportExceptionHandleMBean;
	}

	/**
	 * Sets a MirrorExceptionListener (optional). <p>
	 *
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
		this.readPreference = requireNonNull(readPreference);
		return this;
	}

	/**
	 * Configure optional reloadable properties.
	 */
	public YmerFactory withProperties(Consumer<ReloadableYmerProperties.ReloadablePropertiesBuilder> ymerPropertiesConfigurer) {
		ymerPropertiesConfigurer.accept(ymerPropertiesBuilder);
		return this;
	}

	public SpaceDataSource createSpaceDataSource() {
		return new YmerSpaceDataSource(createSpaceMirrorContext());
	}

	public SpaceSynchronizationEndpoint createSpaceSynchronizationEndpoint() {
		YmerSpaceSynchronizationEndpoint ymerSpaceSynchronizationEndpoint = new YmerSpaceSynchronizationEndpoint(
				createSpaceMirrorContext(),
				ymerPropertiesBuilder.build()
		);
		if (this.exportExceptionHandleMBean) {
			ymerSpaceSynchronizationEndpoint.registerExceptionHandlerMBean();
		}
		if (mirroredObjects.getMirroredObjects().stream().anyMatch(MirroredObject::persistInstanceId)) {
			ymerSpaceSynchronizationEndpoint.registerPersistedInstanceIdCalculationServiceMBean();
		}
		if (applicationContext != null) {
			ymerSpaceSynchronizationEndpoint.setApplicationContext(applicationContext);
		}
		return ymerSpaceSynchronizationEndpoint;
	}

	private SpaceMirrorContext createSpaceMirrorContext() {
		DocumentDb documentDb = DocumentDb.mongoDb(mongoDatabaseSupplier.get(), readPreference);
		DocumentConverter documentConverter = DocumentConverter.mongoConverter(mongoConverter);
		// Set the event publisher to null to avoid deadlocks when loading data in parallel
		if (mongoConverter.getMappingContext() instanceof ApplicationEventPublisherAware) {
			((ApplicationEventPublisherAware)mongoConverter.getMappingContext()).setApplicationEventPublisher(null);
		}
		return new SpaceMirrorContext(mirroredObjects, documentConverter, documentDb, exceptionListener, new Plugins(plugins), numParallelCollections);
	}

}
