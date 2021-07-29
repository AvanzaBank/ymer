package com.avanza.ymer;

import static com.avanza.ymer.MirroredObject.DOCUMENT_INSTANCE_ID;
import static com.avanza.ymer.MirroredObject.DOCUMENT_ROUTING_KEY;
import static com.avanza.ymer.util.GigaSpacesInstanceIdUtil.getInstanceId;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;
import java.util.stream.IntStream;

import org.bson.Document;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.testcontainers.containers.MongoDBContainer;

import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class PersistedInstanceIdRecalculationServiceTest {

	private static final String COLLECTION_NAME = "test_collection";

	@ClassRule
	public static MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:3.6");

	private final MongoDbFactory mongoDbFactory = new SimpleMongoDbFactory(new MongoClientURI(mongoDBContainer.getReplicaSetUrl()));
	private final MongoDatabase database = mongoDbFactory.getDb();
	private final MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

	private final PersistedInstanceIdRecalculationService target;

	public PersistedInstanceIdRecalculationServiceTest() {
		TestSpaceMirrorFactory testSpaceMirrorFactory = new TestSpaceMirrorFactory(mongoDbFactory);
		SpaceSynchronizationEndpoint spaceSynchronizationEndpoint = testSpaceMirrorFactory.createSpaceSynchronizationEndpoint();
		target = ((YmerSpaceSynchronizationEndpoint) spaceSynchronizationEndpoint).getPersistedInstanceIdRecalculationService();
	}

	@Before
	public void setUp() {
		List<Document> documents = IntStream.rangeClosed(1, 1_000)
				.mapToObj(this::createDocument)
				.collect(toList());
		collection.insertMany(documents);
	}

	@Test
	public void shouldUpdateDocumentsWithCorrectInstanceId() {
		int numberOfInstances = 20;

		target.recalculatePersistedInstanceId(COLLECTION_NAME, numberOfInstances);

		MongoDocumentCollection mongoDocumentCollection = new MongoDocumentCollection(collection);
		mongoDocumentCollection.findAll()
				.forEach(document ->
								 assertThat(document.getInteger(DOCUMENT_INSTANCE_ID), equalTo(getInstanceId(document.get(DOCUMENT_ROUTING_KEY), numberOfInstances)))
				);
	}

	private Document createDocument(int id) {
		Document document = new Document("_id", id);
		document.put(DOCUMENT_ROUTING_KEY, id);
		return document;
	}
}