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
package com.avanza.ymer.test;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.UUID;

import org.springframework.beans.BeanUtils;
import org.springframework.util.ReflectionUtils;

import com.gigaspaces.annotation.pojo.SpaceId;

public final class SpaceClassTestHelper {
	private SpaceClassTestHelper() {}

	public static void ensureSpaceId(Object spaceObject) {
		PropertyDescriptor spaceIdProperty = Arrays.stream(BeanUtils.getPropertyDescriptors(spaceObject.getClass()))
				.filter(it -> it.getReadMethod() != null)
				.filter(it -> it.getReadMethod().isAnnotationPresent(SpaceId.class))
				.findFirst()
				.orElseThrow(() -> new AssertionError("Could not find @SpaceId on " + spaceObject.getClass().getSimpleName()));

		Method readMethod = spaceIdProperty.getReadMethod();
		ReflectionUtils.makeAccessible(readMethod);
		if (readMethod.getAnnotation(SpaceId.class).autoGenerate() && ReflectionUtils.invokeMethod(readMethod, spaceObject) == null) {
			String uid = UUID.randomUUID().toString();
			Method writeMethod = spaceIdProperty.getWriteMethod();
			if (writeMethod != null) {
				ReflectionUtils.makeAccessible(writeMethod);
				ReflectionUtils.invokeMethod(writeMethod, spaceObject, uid);
				return;
			}
			Field field = ReflectionUtils.findField(spaceObject.getClass(), spaceIdProperty.getName());
			if (field != null) {
				ReflectionUtils.makeAccessible(field);
				ReflectionUtils.setField(field, spaceObject, uid);
				return;
			}
			throw new AssertionError("Could not set property " + spaceIdProperty.getName());
		}
	}
}
