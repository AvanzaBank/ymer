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

import static com.avanza.ymer.TestSpaceMirrorObjectDefinitions.TEST_SPACE_OBJECT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.avanza.ymer.helper.FakeBatchData;
import com.avanza.ymer.helper.FakeBulkItem;
import com.gigaspaces.sync.DataSyncOperationType;

public class BulkMirroredObjectWriterTest {

	private MockExceptionHandler mockExceptionHandler;
	private BulkMirroredObjectWriter bulkMirroredObjectWriter;
	private final InstanceMetadata testMetadata = new InstanceMetadata(1, null);
	private DocumentDb documentDb;
	private SpaceMirrorContext mirror;

	private static class MockExceptionHandler implements DocumentWriteExceptionHandler {
		private final List<Exception> exceptions = new ArrayList<>();
		private final List<String> descriptions = new ArrayList<>();

		@Override
		public void handleException(Exception exception, String operationDescription) {
			this.exceptions.add(exception);
			this.descriptions.add(operationDescription);
		}

		public void reset() {
			exceptions.clear();
			descriptions.clear();
		}
	}

	@Before
	public void setUp() {
		documentDb = FakeDocumentDb.create();
		mockExceptionHandler = new MockExceptionHandler();
		TestSpaceMirrorObjectDefinitions definitions = new TestSpaceMirrorObjectDefinitions();
		mirror = new SpaceMirrorContext(
				new MirroredObjects(definitions.getMirroredObjectDefinitions().stream(), MirroredObjectDefinitionsOverride.noOverride()),
				TestSpaceObjectFakeConverter.create(),
				documentDb,
				SpaceMirrorContext.NO_EXCEPTION_LISTENER,
				Plugins.empty(),
				1);

		bulkMirroredObjectWriter = new BulkMirroredObjectWriter(
				mirror,
				mockExceptionHandler,
				new MirroredObjectFilterer(mirror)
		);
	}

	@After
	public void tearDown() {
		Configurator.reconfigure();
	}

	@Test
	public void shouldAbortRetriesAfterTooManyFailures() {
		// this test logs a lot of errors, so disable logs temporarily
		Configurator.setLevel(BulkMirroredObjectWriter.class, Level.OFF);

		int numObjects = 10_000;

		TestSpaceObject[] objects = IntStream.range(1, numObjects)
				.mapToObj(i -> new TestSpaceObject("id_" + i, "message" + i))
				.toArray(TestSpaceObject[]::new);

		// This will cause each write to fail as all the objects already exists in DB
		documentDb.getCollection(TEST_SPACE_OBJECT.collectionName())
				.insertAll(Stream.of(objects)
						.map(a -> mirror.toVersionedDocument(a, testMetadata))
						.toArray(Document[]::new));

		// This would lead to a StackOverflowError if the retries are not cancelled
		bulkMirroredObjectWriter.executeBulk(testMetadata, new FakeBatchData(
				Stream.of(objects)
						.map(spaceObject -> new FakeBulkItem(spaceObject, DataSyncOperationType.WRITE))
						.toArray(FakeBulkItem[]::new)
		));

		assertThat(mockExceptionHandler.descriptions, contains(
				containsString("Bulk write failed on a INSERT. This bulk has failed 100 retries"))
		);
	}

	@Test
	public void unexpectedExceptionIsSentToExceptionHandler() {
		FakeDocumentCollection mockCollection = (FakeDocumentCollection) documentDb.getCollection(TEST_SPACE_OBJECT.collectionName());

		RuntimeException testException = new RuntimeException("Unexpected exception from MongoDB");
		mockCollection.setBulkException(() -> testException);

		bulkMirroredObjectWriter.executeBulk(testMetadata, FakeBatchData.create(new FakeBulkItem(
				new TestSpaceObject("id", "message"),
				DataSyncOperationType.WRITE
		)));

		assertThat(mockExceptionHandler.exceptions, contains(testException));
		assertThat(mockExceptionHandler.descriptions, contains("Operation: Bulk write, changes: [INSERT: TestSpaceObject [id=id, message=message]]"));
	}
}
