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

import java.util.Arrays;
import java.util.Collection;

import com.avanza.ymer.YmerInitialLoadIntegrationTest.TestSpaceObjectV1Patch;
import com.avanza.ymer.YmerInitialLoadIntegrationTest.TestSpaceObjectV2Patch;

public class TestSpaceMirrorObjectDefinitions implements MirroredObjectsConfiguration {

	public static final MirroredObjectDefinition<TestSpaceObject> TEST_SPACE_OBJECT =
			MirroredObjectDefinition.create(TestSpaceObject.class)
					.loadDocumentsRouted(true)
					.persistInstanceId(true)
					.documentPatches(new TestSpaceObjectV1Patch(), new TestSpaceObjectV2Patch());

	public static final MirroredObjectDefinition<TestSpaceOtherObject> TEST_SPACE_OTHER_OBJECT =
			MirroredObjectDefinition.create(TestSpaceOtherObject.class)
					.writeBackPatchedDocuments(false)
					.persistInstanceId(configurer -> configurer
							.enabled(true)
							.triggerCalculationOnStartup(false)
					)
					.documentPatches(new TestSpaceObjectV1Patch());

	public static final MirroredObjectDefinition<TestSpaceThirdObject> TEST_SPACE_THIRD_OBJECT =
			MirroredObjectDefinition.create(TestSpaceThirdObject.class)
					.documentPatches(new TestSpaceThirdObject.TestSpaceThirdObjectPatchV1());

	public static final MirroredObjectDefinition<TestSpaceObjectWithCustomRoutingKey> TEST_SPACE_OBJECT_CUSTOM_ROUTINGKEY =
			MirroredObjectDefinition.create(TestSpaceObjectWithCustomRoutingKey.class)
					.persistInstanceId(true);

	public static final MirroredObjectDefinition<TestNonDeleteableSpaceObject> TEST_SPACE_OBJECT_KEEP_PERSISTENT =
			MirroredObjectDefinition.create(TestNonDeleteableSpaceObject.class)
					.keepPersistent(true);

	@Override
	public Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions() {
		return Arrays.asList(
				TEST_SPACE_OBJECT,
				TEST_SPACE_OTHER_OBJECT,
				TEST_SPACE_THIRD_OBJECT,
				TEST_SPACE_OBJECT_CUSTOM_ROUTINGKEY,
				TEST_SPACE_OBJECT_KEEP_PERSISTENT
		);
	}
}
