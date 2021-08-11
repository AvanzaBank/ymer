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
package com.avanza.ymer.junit4;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

@AnalyzeClasses(packages = "com.avanza.ymer", importOptions = { ImportOption.DoNotIncludeJars.class })
class Junit4PackageArchitectureTest {

	@ArchTest
	static final ArchRule classesInJunit4PackageShouldNotDependOnJunit5 =
			noClasses()
					.that().resideInAPackage("com.avanza.ymer.junit4")
					.should()
					.dependOnClassesThat().resideInAPackage("org.junit.jupiter..")
					.orShould()
					.dependOnClassesThat().resideInAPackage("com.avanza.ymer.junit5");

}
