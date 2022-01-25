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
import java.util.List;

import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.MongoDbFactory;

import com.avanza.gs.test.PuConfigurers;
import com.avanza.gs.test.RunningPu;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;

/**
 * This verifies that the application context can be started without cluster properties being defined.
 * Such a setup might be used in test scenarios, but is not expected in a real runtime.
 */
public class YmerMirrorNoClusterPropertiesIntegrationTest {

	@ClassRule
	public static final MirrorEnvironment mirrorEnv = new MirrorEnvironment();

	@Test
	public void shouldStartMirrorWhenInstanceIdPersistingIsEnabled() throws Exception {
		try (RunningPu mirrorPu = PuConfigurers.mirrorPu(ApplicationConfigWithInstanceIdPersisting.class).configure()) {
			// This test should log a lot of warnings, but still start the application context
			mirrorPu.start();
		}
	}

	@Test
	public void shouldStartMirrorWhenInstanceIdPersistingIsDisabled() throws Exception {
		try (RunningPu mirrorPu = PuConfigurers.mirrorPu(ApplicationConfigWithoutInstanceIdPersisting.class).configure()) {
			// This test should start the application context without warnings about current number of instances
			mirrorPu.start();
		}
	}

	@Configuration
	static class TestConfigBase {
		@Bean
		public YmerFactory ymerFactory(Collection<MirroredObjectDefinition<?>> testDefinitions) {
			MongoDbFactory mongoDbFactory = mirrorEnv.getMongoTemplate().getMongoDbFactory();
			return new YmerFactory(
					mongoDbFactory,
					new TestSpaceMongoConverterFactory(mongoDbFactory).createMongoConverter(),
					testDefinitions
			);
		}

		@Bean
		public SpaceSynchronizationEndpoint spaceSynchronizationEndpoint(YmerFactory ymerFactory) {
			return ymerFactory.createSpaceSynchronizationEndpoint();
		}
	}

	@Configuration
	@Import(TestConfigBase.class)
	static class ApplicationConfigWithInstanceIdPersisting {
		@Bean
		public Collection<MirroredObjectDefinition<?>> testDefinitions() {
			return TestSpaceMirrorObjectDefinitions.getDefinitions();
		}
	}

	@Configuration
	@Import(TestConfigBase.class)
	static class ApplicationConfigWithoutInstanceIdPersisting {
		@Bean
		public Collection<MirroredObjectDefinition<?>> testDefinitions() {
			return List.of(TestSpaceMirrorObjectDefinitions.TEST_SPACE_THIRD_OBJECT);
		}
	}

}
