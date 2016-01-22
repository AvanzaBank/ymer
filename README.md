# Ymer Framework
[![][travis img]][travis]
[![][maven img]][maven]
[![][license img]][license]
[![Average time to resolve an issue](http://isitmaintained.com/badge/resolution/AvanzaBank/ymer.svg)](http://isitmaintained.com/project/AvanzaBank/Ymer "Average time to resolve an issue")
[![Percentage of issues still open](http://isitmaintained.com/badge/open/AvanzaBank/ymer.svg)](http://isitmaintained.com/project/AvanzaBank/Ymer "Percentage of issues still open")


Ymer is a MongoDB based `SpaceDataSource` and `SpaceSynchronizationEndpoint` for GigaSpaces with support to apply data migrations during initial load.

## Example Usage
### Use Ymer to define a custom factory for a SpaceDataSource and SpaceSynchronizationEndpoint
```java
public class ExampleMirrorFactory {
	
	/*
	* Ymer uses a MongoDbFactory to "connect" to a given mongo database (by invoking MongoDbFactory#getDb()).
	*/
	private MongoDbFactory mongoDbFactory;

	public SpaceDataSource createSpaceDataSource() {
		return new YmerFactory(mongoDbFactory, createMongoConverter(), getDefinitions()).createSpaceDataSource();
	}
	
	public SpaceSynchronizationEndpoint createSpaceSynchronizationEndpoint() {
		return new YmerFactory(mongoDbFactory, createMongoConverter(), getDefinitions()).createSpaceSynchronizationEndpoint();
	}
	
	/* 
	* Ymer uses the given MongoConverter to convert between space object form and the bson form used to store it
	* in mongo db.
	*/
	private MongoConverter createMongoConverter() {
		DbRefResolver dbRef = new DefaultDbRefResolver(mongoDbFactory);
		return new MappingMongoConverter(dbRef , new MongoMappingContext());
	}
	
	/* 
	* Each MirroredObjectDefinition defines that all space objects of a given type should be persisted
	* using Ymer.
	*/
	private Collection<MirroredObjectDefinition<?>> getDefinitions() {
		return Arrays.asList(
			MirroredObjectDefinition.create(SpaceFruit.class)
		);
	}
	
}
```

### Usage of custom factory in pu.xml

```xml	
<os-core:space id="testSpace" url="/./example-space" mirror="true" space-data-source="spaceDataSource">
	<!-- configuration... -->
</os-core:space>

<!-- Configure SpaceDataSource used for initial load -->
<bean id="mirrorFactory" class="example.mirror.ExampleMirrorFactory"/>
<bean id="spaceDataSource" factory-bean="mirrorFactory" factory-method="createSpaceDataSource"/>
```

### Usage of custom factory in mirror-pu.xml
```xml
<os-core:mirror id="mirrorGigaSpace" url="/./example-space-mirror" space-sync-endpoint="spaceSyncEndpoint">
	<!-- configuration... -->
</os-core:mirror>

<!-- Configure SpaceSynchronizationEndpoint -->
<bean id="mirrorFactory" class="example.mirror.ExampleMirrorFactory"/>
<bean id="spaceSyncEndpoint" factory-bean="mirrorFactory"
							 factory-method="createSpaceSynchronizationEndpoint"/>	
```

### Write DocumentPatches to migrate data on initial load
```java
// First version of SpaceFruit 
public class SpaceFruit {

	@Id
	private String name;
	private String origin;

}

// New version of SpaceFruit adds new property
public class SpaceFruit {

	@Id
	private String name;
	private String origin;
	private boolean organic;

}


// A DocumentPatch is applied by ymer to the raw bson (mongo) document during initial load
class SpaceFruitV1ToV2Patch implements DocumentPatch {

	@Override
	public void apply(BasicDBObject dbObject) {
		// We add a default value for the "organic" property to all existing SpaceFruit's
		dbObject.put("organic", false);
	}

	
	// This patch applies to documents that are on version 1. It patches
	// the document to version 2.
	@Override
	public int patchedVersion() {
		return 1;
	}

}


// Patches are added to the MirroredObjectDefinition
MirroredObjectDefinition.create(SpaceFruit.class)
		        .documentPatches(new SpaceFruitV1ToV2Patch())
```



## License
The Ymer Framework is released under version 2.0 of the [Apache License](http://www.apache.org/licenses/LICENSE-2.0).

[travis]:https://travis-ci.org/AvanzaBank/ymer
[travis img]:https://api.travis-ci.org/AvanzaBank/ymer.svg

[release]:https://github.com/avanzabank/ymer/releases
[release img]:https://img.shields.io/github/release/avanzabank/ymer.svg

[license]:LICENSE
[license img]:https://img.shields.io/badge/License-Apache%202-blue.svg

[maven]:http://search.maven.org/#search|gav|1|g:"com.avanza.ymer"
[maven img]:https://maven-badges.herokuapp.com/maven-central/com.avanza.ymer/ymer/badge.svg
