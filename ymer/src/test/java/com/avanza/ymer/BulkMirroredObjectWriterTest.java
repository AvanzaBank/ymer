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
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.iterableWithSize;

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
import com.avanza.ymer.helper.MirrorExceptionSpy;
import com.gigaspaces.sync.DataSyncOperationType;

public class BulkMirroredObjectWriterTest {

	private FakeDocumentWriteExceptionHandler exceptionHandler;
	private BulkMirroredObjectWriter bulkMirroredObjectWriter;
	private final InstanceMetadata testMetadata = new InstanceMetadata(1, null);
	private DocumentDb documentDb;
	private SpaceMirrorContext mirror;
	private MirrorExceptionSpy mirrorExceptionSpy;

	@Before
	public void setUp() {
		documentDb = FakeDocumentDb.create();
		mirrorExceptionSpy = new MirrorExceptionSpy();
		exceptionHandler = new FakeDocumentWriteExceptionHandler();
		TestSpaceMirrorObjectDefinitions definitions = new TestSpaceMirrorObjectDefinitions();
		mirror = new SpaceMirrorContext(
				new MirroredObjects(definitions.getMirroredObjectDefinitions().stream(), MirroredObjectDefinitionsOverride.noOverride()),
				TestSpaceObjectFakeConverter.create(),
				documentDb,
				mirrorExceptionSpy,
				Plugins.empty(),
				1);

		bulkMirroredObjectWriter = new BulkMirroredObjectWriter(
				mirror,
				exceptionHandler,
				new MirroredObjectFilterer(mirror)
		);
	}

	@After
	public void tearDown() {
		Configurator.reconfigure();
	}

	@Test
	public void shouldTryToWriteAllRowsAfterManyFailures() {
		// this test logs a lot of errors, so disable logs temporarily
		Configurator.setLevel(BulkMirroredObjectWriter.class, Level.OFF);

		TestSpaceObject[] objects = IntStream.rangeClosed(1, 1_000)
				.mapToObj(i -> new TestSpaceObject("id_" + i, "message" + i))
				.toArray(TestSpaceObject[]::new);

		// This will cause each write to fail as all the objects already exists in DB
		documentDb.getCollection(TEST_SPACE_OBJECT.collectionName())
				.insertAll(Stream.of(objects)
						// ensure the first 200 rows fail writing, the remaining should succeed
						.limit(200)
						.map(a -> mirror.toVersionedDocument(a, testMetadata))
						.toArray(Document[]::new));

		// Before bulkWrite, 200 rows should already be written
		assertThat(documentDb.getCollection(TEST_SPACE_OBJECT.collectionName()).findAll().count(), is(200L));

		bulkMirroredObjectWriter.executeBulk(testMetadata, new FakeBatchData(
				Stream.of(objects)
						.map(spaceObject -> new FakeBulkItem(spaceObject, DataSyncOperationType.WRITE))
						.toArray(FakeBulkItem[]::new)
		));

		assertThat(mirrorExceptionSpy.getExceptionCount(), is(200));
		assertThat(mirrorExceptionSpy.getLastException().getMessage(),
				containsString("Bulk write operation error")
		);

		// After bulkWrite, the last 800 rows should be written to db
		assertThat(documentDb.getCollection(TEST_SPACE_OBJECT.collectionName()).findAll().count(), is(1_000L));
	}

	@Test
	public void objectFailingConversionShouldBeFailWithOtherItemsWritten() {
		// this test logs a lot of errors, so disable logs temporarily
		Configurator.setLevel(BulkMirroredObjectWriter.class, Level.OFF);

		TestSpaceObject[] objects = IntStream.rangeClosed(1, 100)
				.mapToObj(i -> {
					TestSpaceObject object = new TestSpaceObject("id_" + i, "message" + i);
					// every second item fails conversion
					if (i % 2 == 0) {
						object.setFailConversion(true);
					}
					return object;
				})
				.toArray(TestSpaceObject[]::new);

		bulkMirroredObjectWriter.executeBulk(testMetadata, new FakeBatchData(
				Stream.of(objects)
						.map(spaceObject -> new FakeBulkItem(spaceObject, DataSyncOperationType.WRITE))
						.toArray(FakeBulkItem[]::new)
		));

		// Every second row should have failed conversion and been added as an exception
		assertThat(mirrorExceptionSpy.getExceptionCount(), is(50));
		assertThat(mirrorExceptionSpy.getLastException().getMessage(),
				startsWith("Could not convert TestSpaceObject")
		);

		// Every second row should have been written to db
		List<Document> objectsInDb = documentDb.getCollection(TEST_SPACE_OBJECT.collectionName()).findAll().collect(toList());
		assertThat(objectsInDb, iterableWithSize(50));
		assertThat(objectsInDb.stream().map(o -> o.getString("_id")).collect(toList()),
				containsInAnyOrder(
						Stream.of(objects)
								.filter(o -> !o.isFailConversion())
								.map(TestSpaceObject::getId)
								.toArray(String[]::new)
				)
		);
	}

	@Test
	public void unexpectedExceptionFromBulkWriteIsSentToExceptionHandler() {
		FakeDocumentCollection mockCollection = (FakeDocumentCollection) documentDb.getCollection(TEST_SPACE_OBJECT.collectionName());

		RuntimeException testException = new RuntimeException("Unexpected exception from MongoDB");
		mockCollection.setMockedBulkException(() -> testException);

		bulkMirroredObjectWriter.executeBulk(testMetadata, FakeBatchData.create(new FakeBulkItem(
				new TestSpaceObject("id", "message"),
				DataSyncOperationType.WRITE
		)));

		assertThat(exceptionHandler.getLastException(), is(testException));
		assertThat(exceptionHandler.getLastOperationDescription(), is("Operation: Bulk write, changes: [INSERT: TestSpaceObject [id=id, message=message]]"));
	}
}
