# Ymer Framework
[![][build img]][build]
[![][maven img]][maven]
[![][license img]][license]
[![Average time to resolve an issue](https://isitmaintained.com/badge/resolution/AvanzaBank/ymer.svg)](https://isitmaintained.com/project/AvanzaBank/Ymer "Average time to resolve an issue")
[![Percentage of issues still open](https://isitmaintained.com/badge/open/AvanzaBank/ymer.svg)](https://isitmaintained.com/project/AvanzaBank/Ymer "Percentage of issues still open")


__Ymer__ is a __MongoDB__ based [SpaceDataSource and SpaceSynchronizationEndpoint](http://docs.gigaspaces.com/xap101/space-persistency.html) for __GigaSpaces__ with support to apply data migrations during initial load.

## Previous verisons

[v1.4.x](https://github.com/AvanzaBank/ymer/tree/v1.4.x) - Based on GigaSpaces 10.1.1 and Java 8


## Usage
A `SpaceDataSource` and `SpaceSynchronizationEndpoint` is created using an `YmerFactory`. Ymer uses __Spring Data MongoDB__:

* A `MongoConverter` is used to convert between space object form and bson (MongoDB) form
* A `MongoDatabaseFactory` is used to create an instance of a mongodb client

You configure `YmerFactory` with a `MongoConverter` that can convert all objects that are intended to be persisted in MongoDB, and you provide a `MongoDatabaseFactory` which effectively defines in what MongoDB instance the objects should be persisted. In addition to a `MongoConverter` and a `MongoDatabaseFactory` instance you also have to provide a collection of `MirroredObjectDefinition's` to define what set of space objects are intended to be persisted in MongoDB. Later in the lifecycle for an application you also use the `MirroredObjectDefinition` to define what patches to (possibly) apply to migrate the data from one version to the next.

You might use the `YmerFactory` directly from your spring configuration (xml or java configuration), or you might implement an application specific factory for a `SpaceDataSource` and `SpaceSynchronizationEndpoint` as the following example illustrates:

### Example
This example shows how to use Ymer to create an application specific factory for a `SpaceDataSource` and `SpaceSynchronizationEndpoint`. Source code and a couple of demo test cases is located in the [examples](examples/) folder. In the example all objects of type `SpaceFruit` will be persisted in MongoDB using Ymer.

#### Application specific factory
```java
public class ExampleMirrorFactory {

	// Ymer uses a MongoDatabaseFactory to create the target instance using MongoDatabaseFactory#getMongoDatabase().
	private MongoDatabaseFactory mongoDatabaseFactory;

	public SpaceDataSource createSpaceDataSource() {
		return new YmerFactory(mongoDatabaseFactory, getMirroredObjectsConfiguration()).createSpaceDataSource();
	}

	public SpaceSynchronizationEndpoint createSpaceSynchronizationEndpoint() {
		return new YmerFactory(mongoDatabaseFactory, getMirroredObjectsConfiguration()).createSpaceSynchronizationEndpoint();
	}


	static MirroredObjectsConfiguration getMirroredObjectsConfiguration() {
		return ExampleMirrorFactory::getDefinitions;
	}

	// Each MirroredObjectDefinition defines that all space objects of a given type should be persisted
	// using Ymer.
	private static Collection<MirroredObjectDefinition<?>> getDefinitions() {
		return List.of(
			MirroredObjectDefinition.create(SpaceFruit.class)
		);
	}
}
```

#### Usage of custom factory in pu.xml

```xml
<os-core:space id="testSpace" url="/./example-space" mirror="true" space-data-source="spaceDataSource">
	<!-- configuration... -->
</os-core:space>

<!-- Configure SpaceDataSource used for initial load -->
<mongo:db-factory id="mongoDbFactory" dbname="exampleDb" mongo-ref="mongo" />
<bean id="mirrorFactory" class="example.mirror.ExampleMirrorFactory"/>
<bean id="spaceDataSource" factory-bean="mirrorFactory" factory-method="createSpaceDataSource"/>
```

#### Usage of custom factory in mirror-pu.xml
```xml
<os-core:mirror id="mirrorGigaSpace" url="/./example-space-mirror" space-sync-endpoint="spaceSyncEndpoint">
	<!-- configuration... -->
</os-core:mirror>

<!-- Configure SpaceSynchronizationEndpoint -->
<mongo:db-factory id="mongoDbFactory" dbname="exampleDb" mongo-ref="mongo" />
<bean id="mirrorFactory" class="example.mirror.ExampleMirrorFactory"/>
<bean id="spaceSyncEndpoint" factory-bean="mirrorFactory"
				factory-method="createSpaceSynchronizationEndpoint"/>	
```
## Data migration
The data migration support in Ymer is designed to achieve the following goals:
* Data migration is performed "just in time" during deployment of the application. No external scripts are required to migrate the data. Migration is performed during __initial load__.
* The application can migrate any database, regardless of age.

Ymer adds a `_formatVersion` property to each persisted document to track the version number for each individual document it stores in MongoDB. The version number is used to decide if a given document requires patching during initial load before using the supplied `MongoConverter` to convert the document into a space object. Note that this design frees the `MongoConvert` instance from handling different versions. The `MongoConverter` only has to know how to convert to and from the latest version of the document format. 

In order for Ymer to be able to migrate data, the application developer writes a `DocumentPatch` every time the data format changes. The `DocumentPatches` associated with a given space type implicitly decides the version of the document format. If there a zero `DocumentPatches`, then the current version is "1", but if there exist a patch for version "1", "2" and "3", then the current version is "4".

#### Example
```java
// First version of SpaceFruit 
public class SpaceFruit {

	@Id
	private String name;
	private String origin;

}

// New version of SpaceFruit adds new "organic" property
public class SpaceFruit {

	@Id
	private String name;
	private String origin;
	private boolean organic;

}


// A DocumentPatch is applied by ymer to the raw bson (mongo) document during initial load
class SpaceFruitV1ToV2Patch implements BsonDocumentPatch {

	@Override
	public void apply(Document dbObject) {
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


## Test support
Ymer includes three test base classes which can be used to verify that the defined `MongoConverter` can convert all mirrored space objects to bson, to test that data-migrations are applied as intended and also to check for `@SpaceClass` annotated classes that are not persisted.

These tests are available for `junit4` in the `ymer-test-junit4` module and for `junit5` in the `ymer-test-junit5`-module.

The examples provided below are using `junit5`.

```java
class ExampleMirrorConverterTest extends YmerConverterTestBase {

	@Override
	protected MirroredObjectsConfiguration getMirroredObjectsConfiguration() {
		return ExampleMirrorFactory.getMirroredObjectsConfiguration();
	}

	@Override
	protected Collection<ConverterTest<?>> testCases() {
		return List.of(
			new ConverterTest<>(new SpaceFruit("Apple", "France", true))
		);
	}
}
```

```java
class ExampleMirrorMigrationTest extends YmerMigrationTestBase {

	@Override
	protected Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions() {
		return ExampleMirrorFactory.getDefinitions();
	}

	@Override
	protected Collection<MigrationTest> testCases() {
		return List.of(
				spaceFruitV1ToV2MigrationTest()
		);
	}
	
	private static MigrationTest spaceFruitV1ToV2MigrationTest() {
		Document v1Doc = new Document();
		v1Doc.put("_id", "apple");
		v1Doc.put("_class", "examples.domain.SpaceFruit");
		v1Doc.put("origin", "Spain");
		
		Document v2Doc = new Document();
		v2Doc.put("_id", "apple");
		v2Doc.put("_class", "examples.domain.SpaceFruit");
		v2Doc.put("origin", "Spain");
		v2Doc.put("organic", false);
		
		return new MigrationTest(v1Doc, v2Doc, 1, SpaceFruit.class);
	}
}
```

```java
class ExampleMirroredTypesTest extends YmerMirroredTypesTestBase {

	@Override
	protected Collection<MirroredObjectDefinition<?>> mirroredObjectDefinitions() {
		return ExampleMirrorFactory.getDefinitions();
	}

	@Override
	protected String basePackageForScanning() {
		return "examples.domain.";
	}
}
```

## Maven
Ymer is packed as a single jar file. Maven users can get Ymer using the following coordinates:
```xml
<dependency>
  <groupId>com.avanza.ymer</groupId>
  <artifactId>ymer</artifactId>
  <version>3.0.0</version>
</dependency>
```

The `junit5` test support is packed in a distinct jar using the following coordinates:
```xml
<dependency>
  <groupId>com.avanza.ymer</groupId>
  <artifactId>ymer-test-junit5</artifactId>
  <version>3.0.0</version>
</dependency>
```

## License
The Ymer Framework is released under version 2.0 of the [Apache License](https://www.apache.org/licenses/LICENSE-2.0).

[build]:https://github.com/AvanzaBank/ymer/actions/workflows/build.yml
[build img]:https://github.com/AvanzaBank/ymer/actions/workflows/build.yml/badge.svg

[release]:https://github.com/avanzabank/ymer/releases
[release img]:https://img.shields.io/github/release/avanzabank/ymer.svg

[license]:LICENSE
[license img]:https://img.shields.io/badge/License-Apache%202-blue.svg

[maven]:https://search.maven.org/#search|gav|1|g:"com.avanza.ymer"
[maven img]:https://maven-badges.herokuapp.com/maven-central/com.avanza.ymer/ymer/badge.svg
