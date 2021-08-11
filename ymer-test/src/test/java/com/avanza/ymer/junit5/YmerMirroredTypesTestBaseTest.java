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
package com.avanza.ymer.junit5;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collection;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.avanza.ymer.MirroredObjectDefinition;
import com.gigaspaces.annotation.pojo.SpaceClass;

class YmerMirroredTypesTestBaseTest {

	// This SpaceClass will be found by the annotation scanner in the tests below
	@SpaceClass
	static class TestSpaceClass {
	}

	static class TestPersistedClassWithoutAnnotation {
	}

	@Test
	void testWhereSpaceClassIsInMirroredObjectDefinitionsShouldPass() {
		YmerMirroredTypesTestBase testInstance = new YmerMirroredTypesTestBase() {
			@Override
			protected Collection<MirroredObjectDefinition<?>> mirroredObjectDefinitions() {
				return singleton(new MirroredObjectDefinition<>(TestSpaceClass.class));
			}

			@Override
			protected String basePackageForScanning() {
				return "com.avanza.ymer.junit5";
			}
		};

		testInstance.allSpaceClassesAreIncludedInYmerFactory();
	}

	@Test
	void testWhereSpaceClassIsNotInMirroredObjectDefinitionsShouldFail() {
		YmerMirroredTypesTestBase testInstance = new YmerMirroredTypesTestBase() {
			@Override
			protected Collection<MirroredObjectDefinition<?>> mirroredObjectDefinitions() {
				return emptySet();
			}

			@Override
			protected String basePackageForScanning() {
				return "com.avanza.ymer.junit5";
			}
		};

		assertThrows(AssertionError.class, testInstance::allSpaceClassesAreIncludedInYmerFactory);
	}

	@Test
	void testExcludingMissingObjectDefinitionFromTestShouldMakeTestPass() {
		YmerMirroredTypesTestBase testInstance = new YmerMirroredTypesTestBase() {
			@Override
			protected Collection<MirroredObjectDefinition<?>> mirroredObjectDefinitions() {
				return emptySet();
			}

			@Override
			protected String basePackageForScanning() {
				return "com.avanza.ymer.junit5";
			}

			@Override
			protected Set<Class<?>> spaceClassesToExcludeFromTest() {
				return singleton(TestSpaceClass.class);
			}
		};

		testInstance.allSpaceClassesAreIncludedInYmerFactory();
	}

	@Test
	void testSearchingInOtherPackageShouldNotFindSpaceClass() {
		YmerMirroredTypesTestBase testInstance = new YmerMirroredTypesTestBase() {
			@Override
			protected Collection<MirroredObjectDefinition<?>> mirroredObjectDefinitions() {
				return emptySet();
			}

			@Override
			protected String basePackageForScanning() {
				return "com.example.";
			}
		};

		testInstance.allSpaceClassesAreIncludedInYmerFactory();
	}

	@Test
	void mirroredTypeAnnotatedWithSpaceClassShouldPass() {
		MirroredObjectDefinition<TestSpaceClass> testSpaceDefinition = new MirroredObjectDefinition<>(TestSpaceClass.class);

		YmerMirroredTypesTestBase testInstance = new YmerMirroredTypesTestBase() {
			@Override
			protected Collection<MirroredObjectDefinition<?>> mirroredObjectDefinitions() {
				return singleton(testSpaceDefinition);
			}

			@Override
			protected String basePackageForScanning() {
				return null;
			}
		};

		testInstance.mirroredTypeIsAnnotatedWithSpaceClass(testSpaceDefinition);
	}

	@Test
	void mirroredTypeNotAnnotatedWithSpaceClassShouldFail() {
		MirroredObjectDefinition<TestPersistedClassWithoutAnnotation> testWithoutAnnotation = new MirroredObjectDefinition<>(TestPersistedClassWithoutAnnotation.class);

		YmerMirroredTypesTestBase testInstance = new YmerMirroredTypesTestBase() {
			@Override
			protected Collection<MirroredObjectDefinition<?>> mirroredObjectDefinitions() {
				return singleton(testWithoutAnnotation);
			}

			@Override
			protected String basePackageForScanning() {
				return null;
			}
		};

		assertThrows(AssertionError.class, () -> testInstance.mirroredTypeIsAnnotatedWithSpaceClass(testWithoutAnnotation));
	}
}
