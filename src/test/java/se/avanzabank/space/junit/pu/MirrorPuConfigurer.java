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
package se.avanzabank.space.junit.pu;

import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.springframework.context.ApplicationContext;

public class MirrorPuConfigurer {
	
	final String puXmlPath;
	Properties properties;
	ApplicationContext parentContext;
	String lookupGroupName = JVMGlobalLus.getLookupGroupName();

	public MirrorPuConfigurer(String puXmlPath) {
		this.puXmlPath = puXmlPath;
	}

	public MirrorPuConfigurer contextProperties(Properties properties) {
		this.properties = properties;
		return this;
	}

	public MirrorPuConfigurer parentContext(ApplicationContext parentContext) {
		this.parentContext = parentContext;
		return this;
	}

	public RunningPu configure() {
		return new RunningPuImpl(new MirrorPu(this));
	}

}
