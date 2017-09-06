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
/*
o * Copyright 2015 Avanza Bank AB
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

import org.springframework.data.mongodb.MongoCollectionUtils;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
/**
 * Holds information about one mirrored space object type.
 *
 * @author Elias Lindholm, Joakim Sahlstrom
 *
 */
public final class MirroredObjectDefinition<T> {

	private final Class<T> mirroredType;
	private String collectionName;
	private DocumentPatch[] patches = new DocumentPatch[0];
	private boolean excludeFromInitialLoad = false;
	private boolean writeBackPatchedDocuments = true;
	private boolean loadDocumentsRouted = false;
	private boolean keepPersistent = false;
	private TemplateFactory customInitialLoadTemplateFactory;

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

    public MirroredObjectDefinition<T> documentPatches(DocumentPatch... patches) {
    	this.patches = patches;
    	return this;
	}
    
    public MirroredObject<T> buildMirroredDocument(MirroredObjectDefinitionsOverride override) {
    	return new MirroredObject<T>(this, override);
    }

	DocumentPatchChain<T> createPatchChain() {
		return new DocumentPatchChain<T>(mirroredType, Arrays.asList(patches));
	}
	
	/**
	 * Indicates that a MirroredObject should not be loaded from the persistent store during InitialLoad. 
	 * In other words no data for this MirroredObject will be present if not loaded through other means.<br/>
	 * <br/>
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
	 * during the last stage of InitialLoad. <br/>
	 * <br/>
	 * When true, persistence-support can utilize several optimizations which reduce system load and memory usage during InitialLoad.
	 * 
	 * Default value is false, indicating that documents will be written back to persistent storage.
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
	 * routing filtering directly in the persistent storage during initial load, drastically reducing the network load since only the correct
	 * subset of data will be transferred to each partition.<br/>
	 * <br/>
	 * Requires documents in the persistent storage to be updated before taking effect. Will take (partial) effect on partially updated collections.<br/>
	 * <br/>
	 * <b>WARNING!</b> Don't activate loadedDocumentRouting if the routing field of a space object is changed, as such changes will not be reflected in the
	 * persistent storage. Make sure to rewrite all space objects before turning routed document loading back on.
	 * 
	 * Default value is false.
	 * 
	 */
	public MirroredObjectDefinition<T> loadDocumentsRouted(boolean loadDocumentsRouted) {
		this.loadDocumentsRouted = loadDocumentsRouted;
		return this;
	}

	boolean loadDocumentsRouted() {
		return this.loadDocumentsRouted;
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