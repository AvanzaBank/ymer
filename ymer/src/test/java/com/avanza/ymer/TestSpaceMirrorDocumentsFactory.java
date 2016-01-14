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

import java.util.Collections;
import java.util.stream.Stream;

import com.avanza.ymer.MirroredDocument.Flag;
import com.avanza.ymer.YmerInitialLoadIntegrationTest.TestSpaceObjectV1Patch;

public class TestSpaceMirrorDocumentsFactory implements MirroredDocumentsFactory {
	
	@Override
	public Stream<MirroredDocument<?>> getDocuments() {
		return Stream.of(
			MirroredDocument.createDocument(TestSpaceObject.class, new TestSpaceObjectV1Patch()),
			MirroredDocument.createDocument(TestSpaceOtherObject.class, Collections.singleton(Flag.DO_NOT_WRITE_BACK_PATCHED_DOCUMENTS), new TestSpaceObjectV1Patch())
		);
	}
	
}
