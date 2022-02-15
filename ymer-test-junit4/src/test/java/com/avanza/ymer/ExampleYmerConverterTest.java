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

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.runners.Parameterized;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;

import com.avanza.ymer.support.JavaInstantReadConverter;
import com.avanza.ymer.support.JavaInstantWriteConverter;

public class ExampleYmerConverterTest extends YmerConverterTestBase {

	public ExampleYmerConverterTest(ConverterTest<?> testCase) {
		super(testCase);
	}

	@Override
	protected Collection<MirroredObjectDefinition<?>> getMirroredObjectDefinitions() {
		return Collections.singletonList(
				new MirroredObjectDefinition<>(ExampleSpaceObjWithInstant.class)
						.loadDocumentsRouted(true)
		);
	}

	@Override
	protected CustomConversions getCustomConversions() {
		return new MongoCustomConversions(List.of(
				new JavaInstantReadConverter(),
				new JavaInstantWriteConverter()
		));
	}

	@Parameterized.Parameters
	public static List<Object[]> testCases() {
		return buildTestCases(
				new ConverterTest<Object>(new ExampleSpaceObjWithInstant("test", Instant.now()))
		);
	}
}
