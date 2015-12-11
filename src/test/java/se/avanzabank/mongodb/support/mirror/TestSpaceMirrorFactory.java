package se.avanzabank.mongodb.support.mirror;

import java.util.Collections;

import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import se.avanzabank.mongodb.support.mirror.MirroredDocument.Flag;
import se.avanzabank.mongodb.support.mirror.VersionedMongoDbExternalDataSourceInitialLoadIntegrationTest.TestSpaceObjectV1Patch;
import se.avanzabank.mongodb.util.LifecycleContainer;

public class TestSpaceMirrorFactory {
	
	private final LifecycleContainer lifecycleContainer = new LifecycleContainer();
	
	public static MirroredDocuments getMirroredDocuments() {
		return new MirroredDocuments(
			new MirroredDocument<>(TestSpaceObject.class, new TestSpaceObjectV1Patch()),
			MirroredDocument.createDocument(TestSpaceOtherObject.class, Collections.singleton(Flag.DoNotWriteBackPatchedDocuments), new TestSpaceObjectV1Patch())
		);
	}
	
	public ManagedDataSourceAndBulkDataPersister create(MongoDbFactory mongoDbFactory) {
		MappingMongoConverter mongoConverter = new MappingMongoConverter(mongoDbFactory, new MongoMappingContext());
		MirroredDocuments mirroredDocuments = getMirroredDocuments();
		ManagedDataSourceAndBulkDataPersister factory = new MongoDbExternalDatasourceFactory(mirroredDocuments, mongoDbFactory, mongoConverter).create();
		lifecycleContainer.add(factory);
		return factory;
	}
	
	
}
