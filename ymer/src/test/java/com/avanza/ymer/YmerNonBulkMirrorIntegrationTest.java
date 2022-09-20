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

import com.avanza.gs.test.RunningPu;

/**
 * These tests use {@link MirroredObjectWriter} for persisting to MongoDB.
 *
 * @deprecated This test is not needed anymore when MirroredObjectWriter is removed
 */
@Deprecated
public class YmerNonBulkMirrorIntegrationTest extends YmerMirrorIntegrationTestBase {

	@Override
	void configureTestCase(RunningPu pu, RunningPu mirrorPu) {
		TestSpaceMirrorFactory spaceMirrorFactory = mirrorPu.getPrimaryApplicationContexts().iterator().next().getBean(TestSpaceMirrorFactory.class);
		spaceMirrorFactory.setUseBulkWrites(false);
	}

}
