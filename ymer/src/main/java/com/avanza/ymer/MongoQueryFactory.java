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

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import java.beans.PropertyDescriptor;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.springframework.beans.BeanUtils;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

/**
 * Builds a mongo {@link Query} object from any object, but maybe preferably from a GigaSpace template :-)
 *
 * @author joasah Joakim Sahlström
 *
 */
class MongoQueryFactory {

	private final ConcurrentMap<Class<?>, List<PropertyDescriptor>> propertyDescriptors = new ConcurrentHashMap<>();
	private final MongoConverter mongoConverter;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mongoMappingContext;

	/**
	 * @param mongoConverter {@link MongoConverter} extracted from the mongo datasource used in gigaspaces
	 */
	public MongoQueryFactory(MongoConverter mongoConverter) {
		this.mongoConverter = requireNonNull(mongoConverter);
		this.mongoMappingContext = mongoConverter.getMappingContext();
	}

	/**
	 * @param template Template object
	 * @return A Spring mongo {@link Query}
	 */
	public Query createMongoQueryFromTemplate(Object template) {
		try {
			Criteria criteria = null;
			MongoPersistentEntity<?> pe = mongoMappingContext.getRequiredPersistentEntity(template.getClass());
			for (PropertyDescriptor pd : getTemplatablePropertyDescriptors(template.getClass())) {
				Object objectValue = pd.getReadMethod().invoke(template);
				if (objectValue == null) {
					continue; // null == accept any value
				}

				String fieldName = pe.getRequiredPersistentProperty(pd.getName()).getFieldName();
				Object mongoValue = mongoConverter.convertToMongoType(objectValue);
				criteria = addCriteria(criteria, fieldName, mongoValue);
			}

			return criteria != null ? new Query(criteria) : new Query();
		} catch (Exception e) {
			throw new CouldNotCreateMongoQueryException(e);
		}
	}

	private Criteria addCriteria(@Nullable Criteria c, String fieldName, Object mongoValue) {
		if (c == null) {
			return Criteria.where(fieldName).is(mongoValue);
		} else {
			return c.and(fieldName).is(mongoValue);
		}
	}

	private List<PropertyDescriptor> getTemplatablePropertyDescriptors(Class<?> type) {
		return propertyDescriptors.computeIfAbsent(type, this::findTemplatablePropertyDescriptors);
	}

	private List<PropertyDescriptor> findTemplatablePropertyDescriptors(Class<?> type) {
		return Stream.of(BeanUtils.getPropertyDescriptors(type))
				.filter(pd -> !isNotTemplatableMethod(pd))
				.collect(toList());
	}

	private boolean isNotTemplatableMethod(PropertyDescriptor pd) {
		return pd.getReadMethod() == null
				|| pd.getReadMethod().getDeclaringClass() == Object.class
				|| pd.getWriteMethod() == null
				|| pd.getName().equals("versionID");
	}

}
