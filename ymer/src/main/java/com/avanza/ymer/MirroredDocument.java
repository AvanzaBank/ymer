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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.springframework.data.mongodb.MongoCollectionUtils;

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.mongodb.BasicDBObject;
/**
 * Holds information about one mirrored space object type.
 *
 * @author Elias Lindholm, Joakim Sahlstrom
 *
 */
public final class MirroredDocument<T> {


	// TODO: rename to MirroredDocumentDefinition? MirroredSpaceObject?

	public enum Flag {
		/**
		 * Indicates that a MirroredDocument should not be loaded from the persistent store during InitialLoad. 
		 * In other words no data for this MirroredDocument will be present if not loaded through other means.<br/>
		 * <br/>
		 * Can be used for collections that are loaded via lazy load. See {@link ReloadableSpaceObject}.
		 */
		EXCLUDE_FROM_INITIAL_LOAD,

		/**
		 * Objects that has been patched (and thus modified) by Ymer during InitialLoad will not be written back to persistent storage
		 * during the last stage of InitialLoad (which is the default behavior).<br/>
		 * <br/>
		 * If this flag is present persistence-support can utilize several optimizations which reduce system load and memory usage during InitialLoad.
		 */
		DO_NOT_WRITE_BACK_PATCHED_DOCUMENTS,

		/**
		 * Adds a routing field to documents that are mirrored to the persistent storage. This field allows objects to be selected with the correct
		 * routing filtering directly in the persistent storage during initial load, drastically reducing the network load since only the correct
		 * subset of data will be transferred to each partition.<br/>
		 * <br/>
		 * Requires documents in the persistent storage to be updated before taking effect. Will take (partial) effect on partially updated collections.<br/>
		 * <br/>
		 * <b>WARNING!</b> This flag may not be present if the routing field of a space object is changed as such changes will not be reflected in the
		 * persistent storage. Make sure to rewrite all space objects before turning the flag back on.
		 */
		LOAD_DOCUMENTS_ROUTED,

		/**
		 * Effectively stops all DELETE operations performed in space from being reflected in the persistent storage. I.e. an object that is deleted
		 * in GigaSpaces will remain in the persistent storage. Usually used in combination with {@link MirroredDocument.Flag.EXCLUDE_FROM_INITIAL_LOAD}
		 */
		KEEP_PERSISTENT
	}
	public static final String DOCUMENT_FORMAT_VERSION_PROPERTY = "_formatVersion";
	public static final String DOCUMENT_ROUTING_KEY = "_routingKey";
	private final DocumentPatchChain<T> patchChain;
	private final RoutingKeyExtractor routingKeyExtractor;
	private final boolean excludeFromInitialLoad;
	private final boolean writeBackPatchedDocuments;
	private final boolean loadDocumentsRouted;
	private final boolean keepPersistent;
    private final String collectionName;

	/**
	 *
	 * @param mirroredType
	 * @param patches
	 */
	public MirroredDocument(Class<T> mirroredType, DocumentPatch... patches) {
		this(mirroredType, Collections.<Flag>emptySet(), patches);
	}

    public MirroredDocument(Class<T> mirroredType, String collectionName, DocumentPatch... patches){
        this(mirroredType, Collections.<Flag>emptySet(), collectionName, patches);
    }

	private MirroredDocument(Class<T> mirroredType, Set<Flag> flags, DocumentPatch... patches) {
		this(new DocumentPatchChain<T>(mirroredType, Arrays.asList(patches)), flags);
	}

    private MirroredDocument(Class<T> mirroredType, Set<Flag> flags, String collectionName, DocumentPatch... patches){
        this(new DocumentPatchChain<T>(mirroredType, Arrays.asList(patches)), flags, collectionName);
    }

    private MirroredDocument(DocumentPatchChain<T> patchChain, Set<Flag> flags) {
        this(patchChain, flags, MongoCollectionUtils.getPreferredCollectionName(patchChain.getMirroredType()));
    }

	private MirroredDocument(DocumentPatchChain<T> patchChain, Set<Flag> flags, String collectionName) {
		this.patchChain = Objects.requireNonNull(patchChain);
		this.routingKeyExtractor = findRoutingKeyMethod(patchChain.getMirroredType());
		this.excludeFromInitialLoad = flags.contains(Flag.EXCLUDE_FROM_INITIAL_LOAD);
        this.writeBackPatchedDocuments = !flags.contains(Flag.DO_NOT_WRITE_BACK_PATCHED_DOCUMENTS);
        this.loadDocumentsRouted = flags.contains(Flag.LOAD_DOCUMENTS_ROUTED);
        this.keepPersistent = flags.contains(Flag.KEEP_PERSISTENT);
        this.collectionName = collectionName;
	}

	public MirroredDocument(MirroredDocumentBuilder<T> mirroredDocumentBuilder) {
		this.patchChain = mirroredDocumentBuilder.createPatchChain();
		this.routingKeyExtractor = findRoutingKeyMethod(patchChain.getMirroredType());
		this.excludeFromInitialLoad = mirroredDocumentBuilder.excludeFromInitialLoad();
        this.writeBackPatchedDocuments = mirroredDocumentBuilder.writeBackPatchedDocuments();
        this.loadDocumentsRouted = mirroredDocumentBuilder.loadDocumentsRouted();
        this.keepPersistent = mirroredDocumentBuilder.keepPersistent();
        this.collectionName = mirroredDocumentBuilder.collectionName();
	}

	public static <T> MirroredDocument<T> createDocument(Class<T> mirroredType, DocumentPatch... patches) {
		return new MirroredDocument<>(mirroredType, Collections.<Flag>emptySet(), patches);
	}

	public static <T> MirroredDocument<T> createDocument(Class<T> mirroredType, Set<Flag> flags, DocumentPatch... patches) {
		return new MirroredDocument<>(mirroredType, flags, patches);
	}

	private RoutingKeyExtractor findRoutingKeyMethod(Class<T> mirroredType) {
		for (Method m : mirroredType.getMethods()) {
			if (m.isAnnotationPresent(SpaceRouting.class) && !m.isAnnotationPresent(SpaceId.class)) {
				return new RoutingKeyExtractor.InstanceMethod(m);
			}
		}
		for (Method m : mirroredType.getMethods()) {
			if (m.isAnnotationPresent(SpaceId.class)) {
				if (RoutingKeyExtractor.GsAutoGenerated.isApplicable(m)) {
					return new RoutingKeyExtractor.GsAutoGenerated(m);
				} else {
					return new RoutingKeyExtractor.InstanceMethod(m);
				}
			}
		}
		throw new IllegalArgumentException("Cannot find @SpaceRouting or @SpaceId method for: " + mirroredType.getName());
	}

	Class<T> getMirroredType() {
		return patchChain.getMirroredType();
	}

	/**
	 * Checks whether a given document requires patching. <p>
	 *
	 * @param dbObject
	 * @throws UnknownDocumentVersionException if the version of the given document is unknown
	 *
	 * @return
	 */
	boolean requiresPatching(BasicDBObject dbObject) {
		int documentVersion = getDocumentVersion(dbObject);
		verifyKnownVersion(documentVersion, dbObject);
		return documentVersion != getCurrentVersion();
	}

	private void verifyKnownVersion(int documentVersion, BasicDBObject dbObject) {
		if (!isKnownVersion(documentVersion)) {
			throw new UnknownDocumentVersionException(String.format("Unknown document version %s, oldest known version is: %s, current version is : %s. document=%s",
					documentVersion, getOldestKnownVersion(), getCurrentVersion(), dbObject));
		}
	}

	int getDocumentVersion(BasicDBObject dbObject) {
		return dbObject.getInt(DOCUMENT_FORMAT_VERSION_PROPERTY, 1);
	}

	void setDocumentVersion(BasicDBObject dbObject, int version) {
		dbObject.put(DOCUMENT_FORMAT_VERSION_PROPERTY, version);
	}

	void setDocumentAttributes(BasicDBObject dbObject, T spaceObject) {
		setDocumentVersion(dbObject);
		if (loadDocumentsRouted) {
			setRoutingKey(dbObject, spaceObject);
		}
	}

	private void setDocumentVersion(BasicDBObject dbObject) {
		dbObject.put(DOCUMENT_FORMAT_VERSION_PROPERTY, getCurrentVersion());
	}

	private void setRoutingKey(BasicDBObject dbObject, T spaceObject) {
		Object routingKey = routingKeyExtractor.getRoutingKey(spaceObject);
		if (routingKey != null) {
			dbObject.put(DOCUMENT_ROUTING_KEY, routingKey.hashCode());
		}
	}

	int getCurrentVersion() {
		if (this.patchChain.isEmpty()) {
			return 1;
		}
		return this.patchChain.getLastPatchInChain().patchedVersion() + 1;
	}

	int getOldestKnownVersion() {
		if (this.patchChain.isEmpty()) {
			return getCurrentVersion();
		}
		return this.patchChain.getFirstPatchInChain().patchedVersion();
	}

	/**
	 * Patches the given document to the current version. <p>
	 *
	 * The argument document will not be mutated. <p>
	 *
	 * @param dbObject
	 * @return
	 */
	BasicDBObject patch(BasicDBObject dbObject) {
		if (!requiresPatching(dbObject)) {
			throw new IllegalArgumentException("Document does not require patching: " + dbObject.toString());
		}
		BasicDBObject patchedDocument = (BasicDBObject) dbObject.copy();
		while (requiresPatching(patchedDocument)) {
			patchToNextVersion(patchedDocument);
		}
		return patchedDocument;
	}

	/**
	 * Patches the given document to the next version by writing mutating the passed in document. <p>
	 *
	 * @param dbObject
	 */
	void patchToNextVersion(BasicDBObject dbObject) {
		if (!requiresPatching(dbObject)) {
			throw new IllegalArgumentException("Document does not require patching: " + dbObject.toString());
		}
		DocumentPatch patch = this.patchChain.getPatch(getDocumentVersion(dbObject));
		patch.apply(dbObject);
		setDocumentVersion(dbObject, patch.patchedVersion() + 1);
	}

	/**
	 * Returns the name of the collection that the underlying documents will be stored in. <p>
	 *
	 * @return
	 */
	public String getCollectionName() {
		return this.collectionName;
	}

	Object getRoutingKey(T spaceObject) {
		return routingKeyExtractor.getRoutingKey(spaceObject);
	}

	boolean isKnownVersion(int documentVersion) {
		return documentVersion >= getOldestKnownVersion() && documentVersion <= getCurrentVersion();
	}

	boolean excludeFromInitialLoad() {
		return excludeFromInitialLoad;
	}

	boolean writeBackPatchedDocuments() {
    	return writeBackPatchedDocuments;
    }

	boolean loadDocumentsRouted() {
		return loadDocumentsRouted;
	}

	public boolean keepPersistent() {
		return keepPersistent;
	}
}