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

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import org.springframework.data.mongodb.MongoCollectionUtils;

import com.mongodb.ReadPreference;

/**
 * Holds information about one mirrored space object type.
 *
 * @author Elias Lindholm, Joakim Sahlstrom
 *
 */
public final class MirroredObjectDefinition<T> {

	private final Class<T> mirroredType;
	private String collectionName;
	private BsonDocumentPatch[] patches = new BsonDocumentPatch[0];
	private boolean excludeFromInitialLoad = false;
	private boolean writeBackPatchedDocuments = true;
	private boolean loadDocumentsRouted = false;
	private final PersistInstanceIdDefinition persistInstanceId = new PersistInstanceIdDefinition();
	private boolean keepPersistent = false;
	private TemplateFactory customInitialLoadTemplateFactory;
	private ReadPreference readPreference;

	public MirroredObjectDefinition(Class<T> mirroredType) {
		this.mirroredType = Objects.requireNonNull(mirroredType);
	}

    public Class<T> getMirroredType() {
        return mirroredType;
    }

    public MirroredObjectDefinition<T> collectionName(String collectionName) {
        this.collectionName = Objects.requireNonNull(collectionName);
		return this;
    }

    public MirroredObjectDefinition<T> documentPatches(BsonDocumentPatch... patches) {
    	this.patches = patches;
    	return this;
	}
    
    public MirroredObject<T> buildMirroredDocument(MirroredObjectDefinitionsOverride override) {
    	return new MirroredObject<>(this, override);
    }

	DocumentPatchChain<T> createPatchChain() {
		return new DocumentPatchChain<>(mirroredType, Arrays.asList(patches));
	}
	
	/**
	 * Indicates that a MirroredObject should not be loaded from the persistent store during InitialLoad. 
	 * In other words no data for this MirroredObject will be present if not loaded through other means.<br>
	 * <br>
	 * Can be used for collections that are loaded via lazy load. See {@link ReloadableSpaceObject}.
	 */
	public MirroredObjectDefinition<T> excludeFromInitialLoad(boolean excludeFromInitialLoad) {
		this.excludeFromInitialLoad = excludeFromInitialLoad;
		return this;
	}

	boolean excludeFromInitialLoad() {
		return this.excludeFromInitialLoad;
	}

	/**
	 * Objects that has been patched (and thus modified) by Ymer during InitialLoad will not be written back to persistent storage
	 * during the last stage of InitialLoad. <br>
	 * <br>
	 * When true, persistence-support can utilize several optimizations which reduce system load and memory usage during InitialLoad.
	 * 
	 * Default value is true, indicating that documents will be written back to persistent storage.
	 */
	public MirroredObjectDefinition<T> writeBackPatchedDocuments(boolean writeBackPatchedDocuments) {
		this.writeBackPatchedDocuments = writeBackPatchedDocuments;
		return this;
	}

	boolean writeBackPatchedDocuments() {
		return this.writeBackPatchedDocuments;
	}
	
	/**
	 * Adds a routing field to documents that are mirrored to the persistent storage. This field allows objects to be selected with the correct
	 * routing filtering directly in the persistent storage during initial setupObjectStream, drastically reducing the network setupObjectStream since only the correct
	 * subset of data will be transferred to each partition.<br>
	 * <br>
	 * Requires documents in the persistent storage to be updated before taking effect. Will take (partial) effect on partially updated collections.<br>
	 * <br>
	 * <b>WARNING!</b> Don't activate loadedDocumentRouting if the routing field of a space object is changed, as such changes will not be reflected in the
	 * persistent storage. Make sure to rewrite all space objects before turning routed document loading back on.
	 * 
	 * Default value is false.
	 */
	public MirroredObjectDefinition<T> loadDocumentsRouted(boolean loadDocumentsRouted) {
		this.loadDocumentsRouted = loadDocumentsRouted;
		return this;
	}

	/**
	 * Sets the read preference for queries against documents in this collection.
	 */
	public MirroredObjectDefinition<T> withReadPreference(ReadPreference readPreference) {
		this.readPreference = Objects.requireNonNull(readPreference);
		return this;
	}

	boolean loadDocumentsRouted() {
		return this.loadDocumentsRouted;
	}

	/**
	 * Whether to persist the current instance id for each document.
	 * This can increase load speed, but requires all persisted partition numbers to be recalculated
	 * when the number of partitions change.
	 *
	 * For more properties related to instance id persisting, see {@link #persistInstanceId(Consumer)}
	 */
	public MirroredObjectDefinition<T> persistInstanceId(boolean enabled) {
		persistInstanceId.enabled(enabled);
		return this;
	}

	/**
	 * Configuration relating to persisting the instance id.
	 * This method accepts a configurer where more detailed configuration is available.
	 * This configuration can be used in the following manner:
	 *
	 * <pre>{@code
	 *   .persistInstanceId(configurer -> configurer
	 *       .enabled(true)
	 *       .triggerCalculationWithDelay(Duration.ofMinutes(60))
	 *   )
	 * }</pre>
	 */
	public MirroredObjectDefinition<T> persistInstanceId(Consumer<PersistInstanceIdDefinition> configurer) {
		configurer.accept(persistInstanceId);
		return this;
	}

	PersistInstanceIdDefinition getPersistInstanceId() {
		return persistInstanceId;
	}

	/**
	 * Effectively stops all DELETE operations performed in space from being reflected in the persistent storage. I.e. an object that is deleted
	 * in GigaSpaces will remain in the persistent storage. Usually used in combination with {@link #excludeFromInitialLoad()}
	 */
	public MirroredObjectDefinition<T> keepPersistent(boolean keepPersistent) {
		this.keepPersistent = keepPersistent;
		return this;
	}

	boolean keepPersistent() {
		return this.keepPersistent;
	}

	String collectionName() {
		return Optional.ofNullable(this.collectionName)
						  .orElseGet(() -> MongoCollectionUtils.getPreferredCollectionName(mirroredType));
	}

	ReadPreference getReadPreference() {
		return readPreference;
	}

	public static <T> MirroredObjectDefinition<T> create(Class<T> mirroredType) {
		return new MirroredObjectDefinition<>(mirroredType);
	}
	
	public MirroredObjectDefinition<T> customInitialLoadTemplateFactory(TemplateFactory templateFactory) {
		this.customInitialLoadTemplateFactory = templateFactory; 
		return this;
	}
	
	public TemplateFactory customInitialLoadTemplateFactory() {
		return customInitialLoadTemplateFactory;
	}

}
