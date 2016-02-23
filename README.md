# Ymer Framework
[![][travis img]][travis]
[![][maven img]][maven]
[![][license img]][license]
[![Average time to resolve an issue](http://isitmaintained.com/badge/resolution/AvanzaBank/ymer.svg)](http://isitmaintained.com/project/AvanzaBank/Ymer "Average time to resolve an issue")
[![Percentage of issues still open](http://isitmaintained.com/badge/open/AvanzaBank/ymer.svg)](http://isitmaintained.com/project/AvanzaBank/Ymer "Percentage of issues still open")


__Ymer__ is a __MongoDB__ based [SpaceDataSource and SpaceSynchronizationEndpoint](http://docs.gigaspaces.com/xap101/space-persistency.html) for __GigaSpaces__ with support to apply data migrations during initial load.


## Usage
A `SpaceDataSource` and `SpaceSynchronizationEndpoint` is created using an `YmerFactory`. Ymer uses __Spring Data MongoDB__:

* A `MongoConverter` is used to convert between space object form and bson (MongoDB) form
* A `MongoDBFactory` is used to create a `com.mongodb.DB` instance

You configure `YmerFactory` with a `MongoConverter` that can convert all objects that are intended to be persisted in MongoDB, and you provide a `MongoDBFactory` which effectively defines in what MongoDB instance the objects should be persisted. In addition to a `MongoConverter` and a `MongoDBFactory` instance you also have to provide a collection of `MirroredObjectDefinition's` to define what set of space objects are intended to be persisted in MongoDB. Later in the lifecycle for an application you also use the `MirroredObjectDefinition` to define what patches to (possibly) apply to migrate the data from one version to the next.

You might use the `YmerFactory` directly from your spring configuration (xml or java configuration), or you might implement an application specific factory for a `SpaceDataSource` and `SpaceSynchronizationEndpoint` as the following example illustrates:

### Example
This example shows how to use Ymer to create an application specific factory for a `SpaceDataSource` and `SpaceSynchronizationEndpoint`. Source code and a couple of demo test cases is located in the [examples](examples/) folder. In the example all objects of type `SpaceFruit` will be persisted in MongoDB using Ymer.

#### Application specific factory
```java
public class ExampleMirrorFactory {
	
	// Ymer uses a MongoDbFactory to create the target `cm.mongodb.DB` instance using MongoDbFactory#getDb().
	private MongoDbFactory mongoDbFactory;

	public SpaceDataSource createSpaceDataSource() {
		return new YmerFactory(mongoDbFactory, createMongoConverter(), getDefinitions()).createSpaceDataSource();
	}
	
	public SpaceSynchronizationEndpoint createSpaceSynchronizationEndpoint() {
		return new YmerFactory(mongoDbFactory, createMongoConverter(), getDefinitions()).createSpaceSynchronizationEndpoint();
	}
	
	 
	// Ymer uses the given MongoConverter to convert between space object form and the bson form used to store it
	// in mongo db.
	private MongoConverter createMongoConverter() {
		DbRefResolver dbRef = new DefaultDbRefResolver(mongoDbFactory);
		return new MappingMongoConverter(dbRef , new MongoMappingContext());
	}
	
	
	// Each MirroredObjectDefinition defines that all space objects of a given type should be persisted
	// using Ymer.
	private Collection<MirroredObjectDefinition<?>> getDefinitions() {
		return Arrays.asList(
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


## Test support
Ymer includes three test base classes (located in the `ymer-test` module) which can be used to verify that the defined `MongoConverter` can convert all mirrored space objects to bson, that data-migrations are applied as intended and that
the latest known version of a document can be unmarshalled into a space object:


```java
public class ExampleMirrorConverterTest extends YmerConverterTestBase {

	public ExampleMirrorConverterTest(ConverterTest<?> testCase) {
		super(testCase);
	}

	@Override
	protected Collection<MirroredObjectDefinition<?>> getMirroredDocumentDefinitions() {
		return ExampleMirrorFactory.getDefinitions();
	}

	@Override
	protected MongoConverter createMongoConverter(MongoDbFactory mongoDbFactory) {
		return ExampleMirrorFactory.createMongoConverter(mongoDbFactory);
	}
	
	@Parameters
	public static List<Object[]> testCases() {
		return buildTestCases(
			new ConverterTest<>(new SpaceFruit("Apple", "France", true))
		);
	}
	
}
```

```java
public class ExampleMirrorMigrationTest extends YmerMigrationTestBase {

	public ExampleMirrorMigrationTest(MigrationTest testCase) {
		super(testCase);
	}

	@Override
	protected Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions() {
		return ExampleMirrorFactory.getDefinitions();
	}

	@Parameters
	public static List<Object[]> testCases() {
		return buildTestCases(
				spaceFruitV1ToV2MigrationTest()
		);
	}
	
	private static MigrationTest spaceFruitV1ToV2MigrationTest() {
		BasicDBObject v1Doc = new BasicDBObject();
		v1Doc.put("_id", "apple");
		v1Doc.put("_class", "examples.domain.SpaceFruit");
		v1Doc.put("origin", "Spain");
		
		BasicDBObject v2Doc = new BasicDBObject();
		v2Doc.put("_id", "apple");
		v2Doc.put("_class", "examples.domain.SpaceFruit");
		v2Doc.put("origin", "Spain");
		v2Doc.put("organic", false);
		
		return new MigrationTest(v1Doc, v2Doc, 1, SpaceFruit.class);
	}
}
```

```java
public class ExampleMirrorUnmarshallTest extends YmerUnmarshallTestBase {

	public ExampleMirrorUnmarshallTest(UnmarshallTest testCase) {
		super(testCase);
	}

	@Override
	protected Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions() {
		return ExampleMirrorFactory.getDefinitions();
	}

	@Override
	protected MongoConverter createMongoConverter(MongoDbFactory mongoDbFactory) {
		return ExampleMirrorFactory.createMongoConverter(mongoDbFactory);
	}

	@Parameters
	public static List<Object[]> testCases() {
		return buildTestCases(
				spaceFruitV2UnmarshallTest()
		);
	}

	private static UnmarshallTest spaceFruitV2UnmarshallTest() {
		BasicDBObject v2Doc = new BasicDBObject();
		v2Doc.put("_id", "apple");
		v2Doc.put("_class", "examples.domain.SpaceFruit");
		v2Doc.put("origin", "Spain");
		v2Doc.put("organic", false);
		v2Doc.put("_formatVersion", 2);

		SpaceFruit expectedSpaceFruit = new SpaceFruit();
		expectedSpaceFruit.setId("apple");
		expectedSpaceFruit.setOrigin("Spain");
		expectedSpaceFruit.setOrganic(false);

		return new UnmarshallTest(v2Doc, expectedSpaceFruit);
	}

}
```

## Maven
Ymer is packed as a single jar file. Maven users can get Ymer using the following coordinates:
```xml
<dependency>
  <groupId>com.avanza.ymer</groupId>
  <artifactId>ymer</artifactId>
  <version>1.1.1</version>
</dependency>
```

The test support is packed in a distinct jar using the following coordinates:
```xml
<dependency>
  <groupId>com.avanza.ymer</groupId>
  <artifactId>ymer-test</artifactId>
  <version>1.1.1</version>
</dependency>
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
