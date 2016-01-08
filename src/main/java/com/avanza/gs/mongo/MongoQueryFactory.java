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
package com.avanza.gs.mongo;

import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import org.springframework.beans.BeanUtils;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

/**
 * Builds a mongo {@link Query} object from any object, but maybe preferably from a GigaSpace template :-)
 *
 * @author joasah Joakim Sahlström
 *
 */
public class MongoQueryFactory {

	private final HashMap<Class<?>, List<PropertyDescriptor>> propertyDescriptors = new HashMap<Class<?>, List<PropertyDescriptor>>();
	private final MongoMappingContext mongoMappingContext;
	private final MongoConverter mongoConverter;

	/**
	 * @param mongoConverter {@link MongoConverter} extracted from the mongo datasource used in gigaspaces
	 */
	public MongoQueryFactory(MongoConverter mongoConverter) {
		this.mongoConverter = Objects.requireNonNull(mongoConverter);
		this.mongoMappingContext = (MongoMappingContext) mongoConverter.getMappingContext();
	}

	/**
	 * @param template Template object
	 * @return A Spring mongo {@link Query}
	 */
	public Query createMongoQueryFromTemplate(Object template) {
		try {
			Criteria criteria = null;
			BasicMongoPersistentEntity<?> pe = mongoMappingContext.getPersistentEntity(template.getClass());
			for (PropertyDescriptor pd : getTemplatablePropertyDescriptors(template.getClass())) {
				Object objectValue = pd.getReadMethod().invoke(template);
				if (objectValue == null) {
					continue; // null == accept any value
				}

				String fieldName = pe.getPersistentProperty(pd.getName()).getFieldName();
				Object mongoValue = mongoConverter.convertToMongoType(objectValue);
				criteria = addCriteria(criteria, fieldName, mongoValue);
			}

			return criteria != null ? new Query(criteria) : new Query();
		} catch (Exception e) {
			throw new CouldNotCreateMongoQueryException(e);
		}
	}

	private Criteria addCriteria(Criteria c, String fieldName, Object mongoValue) {
		if (c == null) {
			return Criteria.where(fieldName).is(mongoValue);
		} else {
			return c.and(fieldName).is(mongoValue);
		}
	}

	private List<PropertyDescriptor> getTemplatablePropertyDescriptors(Class<?> type) {
		if (!propertyDescriptors.containsKey(type)) {
			propertyDescriptors.put(type, findTemplatablePropertyDescriptors(type));
		}
		return propertyDescriptors.get(type);
	}

	private List<PropertyDescriptor> findTemplatablePropertyDescriptors(Class<?> type) {
		ArrayList<PropertyDescriptor> result = new ArrayList<PropertyDescriptor>();
		for (PropertyDescriptor pd : BeanUtils.getPropertyDescriptors(type)) {
			if (!isNotTemplatableMethod(pd)) {
				result.add(pd);
			}
		}
		return result;
	}

	private boolean isNotTemplatableMethod(PropertyDescriptor pd) {
		return pd.getReadMethod() == null
				|| pd.getReadMethod().getDeclaringClass() == Object.class
				|| pd.getWriteMethod() == null
				|| pd.getName().equals("versionID");
	}

}
