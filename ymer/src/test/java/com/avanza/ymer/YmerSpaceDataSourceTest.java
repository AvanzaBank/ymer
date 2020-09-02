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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.awaitility.Awaitility;
import org.bson.Document;
import org.junit.Test;
import org.openspaces.core.cluster.ClusterInfo;
import org.springframework.data.mongodb.core.query.Query;
import com.avanza.ymer.YmerSpaceDataSource.InitialLoadCompleteDispatcher;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.gigaspaces.datasource.DataIterator;
import com.mongodb.BasicDBObject;

public class YmerSpaceDataSourceTest {

	private final Integer partitionId = 1;
	private final int numberOfInstances = 2;
	InitialLoadCompleteDispatcher doneDistpacher = new InitialLoadCompleteDispatcher();

	@Test
	public void documentsMustNotBeWrittenToDbBeforeAllElementsAreLoaded() throws Exception {
		DocumentPatch[] patches = { new FakeSpaceObjectV1Patch() };
		MirroredObject<FakeSpaceObject> patchedMirroredDocument = MirroredObjectDefinition.create(FakeSpaceObject.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		DocumentDb fakeDb = FakeDocumentDb.create();
		SpaceMirrorContext spaceMirror = new SpaceMirrorContext(new MirroredObjects(patchedMirroredDocument), FakeDocumentConverter.create(), fakeDb, SpaceMirrorContext.NO_EXCEPTION_LISTENER, Plugins.empty(), 1);
		YmerSpaceDataSource ymerSpaceDataSource = new YmerSpaceDataSource(spaceMirror);
		ymerSpaceDataSource.setClusterInfo(new ClusterInfo("", partitionId, null, numberOfInstances, 0));

		DocumentCollection documentCollection = fakeDb.getCollection(patchedMirroredDocument.getCollectionName());
		Document doc1 = new Document();
		doc1.put("_id", 1);
		doc1.put("spaceRouting", 1);

		Document doc2 = new Document();
		doc2.put("_id", 2);
		doc2.put("spaceRouting", 2);

		Document doc3 = new Document();
		doc3.put("_id", 3);
		doc3.put("spaceRouting", 3);

		documentCollection.insert(doc1);
		documentCollection.insert(doc2);
		documentCollection.insert(doc3);

		
		Stream<FakeSpaceObject> loadInitialLoadData = ymerSpaceDataSource.load(patchedMirroredDocument, doneDistpacher);
		assertEquals(1, Iterables.sizeOf(loadInitialLoadData));
	}

	@Test
	public void loadsAndPatchesASingleDocumentById() throws Exception {
		DocumentPatch[] patches = { new FakeSpaceObjectV1Patch() };
		MirroredObject<TestReloadableSpaceObject> mirroredObject = MirroredObjectDefinition.create(TestReloadableSpaceObject.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
		DocumentDb documentDb = FakeDocumentDb.create();
		SpaceMirrorContext spaceMirror = new SpaceMirrorContext(
				new MirroredObjects(mirroredObject),
				TestSpaceObjectFakeConverter.create(),
				documentDb,
				SpaceMirrorContext.NO_EXCEPTION_LISTENER,
				Plugins.empty(),
				1);
		YmerSpaceDataSource externalDataSourceForPartition1 = new YmerSpaceDataSource(spaceMirror);
		externalDataSourceForPartition1.setClusterInfo(new ClusterInfo("", partitionId, null, numberOfInstances, 0));

		DocumentCollection documentCollection = documentDb.getCollection(mirroredObject.getCollectionName());
		Document doc1 = new Document();
		doc1.put("_id", 1);
		doc1.put("spaceRouting", 1);
		doc1.put("versionID", 1);

		Document doc2 = new Document();
		doc2.put("_id", 2);
		doc2.put("spaceRouting", 2);
		doc2.put("versionID", 1);

		Document doc3 = new Document();
		doc3.put("_id", 3);
		doc3.put("spaceRouting", 3);
		doc3.put("versionID", 1);

		documentCollection.insert(doc1);
		documentCollection.insert(doc2);
		documentCollection.insert(doc3);
		assertNotNull(externalDataSourceForPartition1.reloadObject(TestReloadableSpaceObject.class, 2));

		Document dbObject = documentDb.getCollection(mirroredObject.getCollectionName()).findById(2);
		assertFalse(mirroredObject.requiresPatching(new Document(dbObject)));
	}


	@Test
	public void testLoggning(){
        final TestAppender appender = new TestAppender();
        final Logger logger = Logger.getLogger(YmerSpaceDataSource.class);
		try{
		    logger.setLevel(Level.INFO);
            logger.addAppender(appender);

            DocumentPatch[] patches = {new FakeSpaceObjectV1Patch()};
            MirroredObject<FakeSpaceObject> patchedMirroredDocument = MirroredObjectDefinition.create(FakeSpaceObject.class).documentPatches(patches).buildMirroredDocument(MirroredObjectDefinitionsOverride.noOverride());
            DocumentDb fakeDb = FakeDocumentDb.create();
            SpaceMirrorContext spaceMirror = new SpaceMirrorContext(new MirroredObjects(patchedMirroredDocument), FakeDocumentConverter.create(), fakeDb, SpaceMirrorContext.NO_EXCEPTION_LISTENER, Plugins.empty(), 1);
            YmerSpaceDataSource ymerSpaceDataSource = new YmerSpaceDataSource(spaceMirror);
            ymerSpaceDataSource.setClusterInfo(new ClusterInfo("", partitionId, null, numberOfInstances, 0));

            DocumentCollection documentCollection = fakeDb.getCollection(patchedMirroredDocument.getCollectionName());
			Document doc2 = new Document();
            doc2.put("_id", 2);
            doc2.put("spaceRouting", 2);

            documentCollection.insert(doc2);

            DataIterator<Object> objectDataIterator = ymerSpaceDataSource.initialDataLoad();
            while (objectDataIterator.hasNext()) {
				objectDataIterator.next();
			}
			Awaitility.await().until(() ->
					appender.getLog().stream()
							.filter(e -> e.getMessage().toString().startsWith("Loaded 1 documents from fakeSpaceObject"))
									.count()
							== 1);
        } finally {
            logger.removeAppender(appender);
		}
	}

	private static class FakeSpaceObject {

		private int id;
		private int spaceRouting;

		public FakeSpaceObject() {
		}

		public void setId(int id) {
			this.id = id;
		}

		@SpaceRouting
		public int getSpaceRouting() {
			return spaceRouting;
		}

		public void setSpaceRouting(int routingKey) {
			this.spaceRouting = routingKey;
		}

		@Override
		public int hashCode() {
			return toString().hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			return toString().equals(obj.toString());
		}

		@Override
		public String toString() {
			return "FakeSpaceObject [id=" + id + ", spaceRouting=" + spaceRouting + "]";
		}

	}

	private static class FakeSpaceObjectV1Patch implements DocumentPatch {

		@Override
		public void apply(BasicDBObject dbObject) {
			dbObject.put("patched", true);
		}

		@Override
		public int patchedVersion() {
			return 1;
		}

	}

	private static class FakeDocumentConverter implements DocumentConverter.Provider {

		@SuppressWarnings("unchecked")
		@Override
		public <T> T convert(Class<T> toType, Document document) {
			FakeSpaceObject spaceObject = new FakeSpaceObject();

			Integer spaceRouting = Optional.ofNullable(document.getInteger("spaceRouting"))
										   .orElseThrow(() -> new NullPointerException("no value for: spaceRouting"));

			Integer id = Optional.ofNullable(document.getInteger("_id"))
								 .orElseThrow(() -> new NullPointerException("no value for: _id"));
								 ;
			spaceObject.setSpaceRouting(spaceRouting);
			spaceObject.setId(id);
			return (T) spaceObject;
		}

		@Override
		public Document convertToBsonDocument(Object type) {
			return null;
		}

		public static DocumentConverter create() {
			return DocumentConverter.create(new FakeDocumentConverter());
		}

		@Override
		public Object convert(Object type) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Query toQuery(Object template) {
			throw new UnsupportedOperationException();
		}
	}

	class TestAppender extends AppenderSkeleton {

		private final List<LoggingEvent> log = new ArrayList<>();

		@Override
		public boolean requiresLayout() {
			return false;
		}

		@Override
		protected void append(final LoggingEvent loggingEvent) {
			log.add(loggingEvent);
		}

		@Override
		public void close() {
		}

		List<LoggingEvent> getLog() {
			return new ArrayList<>(log);
		}
	}

}
