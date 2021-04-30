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

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.junit.Test;

import com.gigaspaces.annotation.pojo.SpaceClass;

public class YmerMirroredObjectsTestBaseTest {

	// This SpaceClass will be found by the annotation scanner in the tests below
	@SpaceClass
	static class TestSpaceClass {
	}

	@Test
	public void testWhereSpaceClassIsInMirroredObjectDefinitionsShouldPass() {
		YmerMirroredObjectsTestBase testInstance = new YmerMirroredObjectsTestBase() {
			@Override
			protected Collection<MirroredObjectDefinition<?>> mirroredObjectDefinitions() {
				return Collections.singleton(new MirroredObjectDefinition<>(TestSpaceClass.class));
			}

			@Override
			protected String basePackageForScanning() {
				return "com.avanza.ymer.";
			}
		};

		testInstance.allSpaceClassesAreIncludedInYmerFactory();
	}

	@Test(expected = AssertionError.class)
	public void testWhereSpaceClassIsNotInMirroredObjectDefinitionsShouldFail() {
		YmerMirroredObjectsTestBase testInstance = new YmerMirroredObjectsTestBase() {
			@Override
			protected Collection<MirroredObjectDefinition<?>> mirroredObjectDefinitions() {
				return Collections.emptySet();
			}

			@Override
			protected String basePackageForScanning() {
				return "com.avanza.ymer.";
			}
		};

		testInstance.allSpaceClassesAreIncludedInYmerFactory();
	}

	@Test
	public void testExcludingMissingObjectDefinitionFromTestShouldMakeTestPass() {
		YmerMirroredObjectsTestBase testInstance = new YmerMirroredObjectsTestBase() {
			@Override
			protected Collection<MirroredObjectDefinition<?>> mirroredObjectDefinitions() {
				return Collections.emptySet();
			}

			@Override
			protected String basePackageForScanning() {
				return "com.avanza.ymer.";
			}

			@Override
			protected Set<Class<?>> spaceClassesToExcludeFromTest() {
				return Collections.singleton(TestSpaceClass.class);
			}
		};

		testInstance.allSpaceClassesAreIncludedInYmerFactory();
	}

	@Test
	public void testSearchingInOtherPackageShouldNotFindSpaceClass() {
		YmerMirroredObjectsTestBase testInstance = new YmerMirroredObjectsTestBase() {
			@Override
			protected Collection<MirroredObjectDefinition<?>> mirroredObjectDefinitions() {
				return Collections.emptySet();
			}

			@Override
			protected String basePackageForScanning() {
				return "com.example.";
			}
		};

		testInstance.allSpaceClassesAreIncludedInYmerFactory();
	}
}
