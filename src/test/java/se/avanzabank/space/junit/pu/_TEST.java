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

import org.openspaces.core.space.UrlSpaceConfigurer;

public class _TEST {
	
	public static void main(String[] args) {
		new UrlSpaceConfigurer("/./my-space").lookupGroups("kalle").create();
		new UrlSpaceConfigurer("/./my-space-2").lookupGroups("kalle").create();
		
//		new UrlSpaceConfigurer("jini://*/*/my-space-2").lookupGroups("palle").create();
		new UrlSpaceConfigurer("jini://*/*/my-space").lookupGroups("kalle").create();
		System.out.println("DONE");
		System.exit(0);
	}

}
