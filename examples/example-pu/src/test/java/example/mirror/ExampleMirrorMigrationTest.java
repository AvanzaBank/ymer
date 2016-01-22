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
package example.mirror;

import java.util.Collection;
import java.util.List;

import org.junit.runners.Parameterized.Parameters;

import com.avanza.ymer.MirroredObjectDefinition;
import com.avanza.ymer.YmerMigrationTestBase;
import com.mongodb.BasicDBObject;

import example.domain.SpaceFruit;

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
