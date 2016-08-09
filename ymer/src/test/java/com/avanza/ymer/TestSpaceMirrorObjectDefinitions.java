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

public class TestSpaceMirrorObjectDefinitions  {

	public Collection<MirroredObjectDefinition<?>> getDefinitions() {
		return Arrays.asList(
				MirroredObjectDefinition.create(TestSpaceObject.class).documentPatches(new TestSpaceObjectV1Patch()),
				MirroredObjectDefinition.create(TestSpaceOtherObject.class).writeBackPatchedDocuments(false).documentPatches(new TestSpaceObjectV1Patch()),
				MirroredObjectDefinition.create(TestSpaceThirdObject.class).documentPatches(new TestSpaceThirdObject.TestSpaceThirdObjectPatchV1())
		);
	}

}
