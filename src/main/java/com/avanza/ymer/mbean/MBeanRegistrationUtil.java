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
package com.avanza.ymer.mbean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Kristoffer Erlandsson (krierl), kristoffer.erlandsson@avanzabank.se
 */
public class MBeanRegistrationUtil {
	
	private static final Logger log = LoggerFactory.getLogger(MBeanRegistrationUtil.class);
	
	public static void registerExceptionHandlerMBean(MBeanRegistrator registrator, Object exceptionHandler) {
		try {
			registerMBean(registrator, exceptionHandler);
		} catch (Exception e) {
			log.warn("Exception handler MBean registration failed", e);
		}
	}

	private static void registerMBean(MBeanRegistrator registrator, Object exceptionHandler) {
		String name = "se.avanzabank.space.mirror:type=DocumentWriteExceptionHandler,name=documentWriteExceptionHandler";
		registrator.registerMBean(exceptionHandler, name);
	}
}
