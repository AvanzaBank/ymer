package se.avanzabank.mongodb.support.mirror;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.BasicDBObject;

import se.avanzabank.mongodb.support.mirror.MirroredDocument.Flag;
import se.avanzabank.space.junit.pu.PuConfigurers;
import se.avanzabank.space.junit.pu.RunningPu;

public class VersionedMongoDbExternalDataSourceInitialLoadIntegrationTest {
	
	static MirroredDocument<TestSpaceObject> mirroredDocument = new MirroredDocument<>(TestSpaceObject.class, new TestSpaceObjectV1Patch());
	static MirroredDocument<TestSpaceOtherObject> mirroredOtherDocument = MirroredDocument.createDocument(
			TestSpaceOtherObject.class, 
			Collections.<Flag>singleton(Flag.DoNotWriteBackPatchedDocuments), 
			new TestSpaceObjectV1Patch());
	static MirroredDocuments mirroredDocuments = new MirroredDocuments(mirroredDocument, mirroredOtherDocument);
	
	@ClassRule
	public static MirrorEnvironmentRunner mirrorEnv = new MirrorEnvironmentRunner(mirroredDocuments);
	
	public RunningPu pu = PuConfigurers.partitionedPu("classpath:/mongo-mirror-integration-test-pu.xml")
									   .numberOfPrimaries(1)
									   .startAsync(false)
									   .parentContext(mirrorEnv.getMongoClientContext())
									   .contextProperties(new Properties() {{
										   setProperty("databasename", mirrorEnv.getDatabaseName());
//										   setProperty("mongouri", mirrorEnv.getMongoUri());
									   }})
//									   .("testSpaceGigaSpace")
									   .configure();
	@After
	public void cleanup() throws Exception {
		mirrorEnv.dropAllMongoCollections();
		pu.close();
	}

	@Test
	public void migratesOldDocumentOnInitialLoad() throws Exception {
		BasicDBObject spaceObjectV1 = new BasicDBObject();
		spaceObjectV1.put("_id", "id_v1");
		spaceObjectV1.put("message", "Msg_V1");
		
		BasicDBObject spaceObjectV2 = new BasicDBObject();
		spaceObjectV2.put("_id", "id_v2");
		spaceObjectV2.put("message", "Msg_V2");
		spaceObjectV2.put(MirroredDocument.DOCUMENT_FORMAT_VERSION_PROPERTY, 2);
		
		BasicDBObject spaceOtherObject = new BasicDBObject();
		spaceOtherObject.put("_id", "otherId");
		spaceOtherObject.put("message", "Msg_V1");
		
		MongoTemplate mongoTemplate = mirrorEnv.getMongoTemplate();
		mongoTemplate.getCollection(mirroredDocument.getCollectionName()).insert(spaceObjectV1);
		mongoTemplate.getCollection(mirroredDocument.getCollectionName()).insert(spaceObjectV2);
		mongoTemplate.getCollection(mirroredOtherDocument.getCollectionName()).insert(spaceOtherObject);

		pu.start();
		
		// Verify SpaceObject
		GigaSpace gigaSpace = pu.getClusteredGigaSpace();
		assertEquals(2, gigaSpace.count(new TestSpaceObject()));
		TestSpaceObject testSpaceObject = gigaSpace.readById(TestSpaceObject.class, "id_v1");
		assertEquals("patched_Msg_V1", testSpaceObject.getMessage());
		
		List<BasicDBObject> allDocs = mongoTemplate.findAll(BasicDBObject.class, mirroredDocument.getCollectionName());
		assertEquals(2, allDocs.size());
		assertEquals(2, mirroredDocument.getDocumentVersion(allDocs.get(0)));
		assertEquals(2, mirroredDocument.getDocumentVersion(allDocs.get(1)));
		
		// Verify SpaceOtherObject
		assertEquals(1, gigaSpace.count(new TestSpaceOtherObject()));
		TestSpaceOtherObject testSpaceOtherObject = gigaSpace.readById(TestSpaceOtherObject.class, "otherId");
		assertEquals("patched_Msg_V1", testSpaceOtherObject.getMessage());
		
		List<BasicDBObject> allOtherDocs = mongoTemplate.findAll(BasicDBObject.class, mirroredOtherDocument.getCollectionName());
		assertEquals(1, allOtherDocs.size());
		assertEquals(1, mirroredOtherDocument.getDocumentVersion(allOtherDocs.get(0)));
	}
	
	public static class TestSpaceObjectV1Patch implements DocumentPatch {
		@Override
		public void apply(BasicDBObject dbObject) {
			dbObject.put("message", "patched_" + dbObject.getString("message"));
		}

		@Override
		public int patchedVersion() {
			return 1;
		}
		
	}
	
}
