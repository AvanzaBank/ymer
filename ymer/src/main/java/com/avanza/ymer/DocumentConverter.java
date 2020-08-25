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

import java.util.Objects;

import org.bson.Document;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Query;

/**
 * Strategy for converting a mongo document into a domain object (ie the object
 * that at some times was mirrored). <p>
 *
 * @author Elias Lindholm (elilin)
 *
 */
final class DocumentConverter {

    private final Provider provider;

    private DocumentConverter(Provider provider) {
        this.provider = provider;
    }

    static DocumentConverter mongoConverter(MongoConverter mongoConverter) {
        return new DocumentConverter(new MongoConverterDocumentConverter(mongoConverter));
    }

    static DocumentConverter create(Provider provider) {
        return new DocumentConverter(provider);
    }

    /**
     * Reads the given Document and convert it to the given type. <p>
     *
     * @param toType
     * @param document
     * @return
     */
    <T> T convert(Class<T> toType, Document document) {
        return provider.convert(toType, document);
    }

    /**
     * Converts the given object to a BSON document. <p>
     *
     * @param type
     * @return
     */
    Document convertToBsonDocument(Object type) {
        return provider.convertToBsonDocument(type);
    }

    /**
     * Converts the given object to the corresponding mongo object (may or may not be a DBObject)
     *
     * @param type
     * @return
     */
    Object convertToMongoObject(Object type) {
        return provider.convert(type);
    }

    Query toQuery(Object template) {
        return provider.toQuery(template);
    }

    interface Provider {

        /**
         * Reads the given Document and convert it to the given type. <p>
         *
         * @param toType
         * @param document
         * @return
         */
        <T> T convert(Class<T> toType, Document document);

        /**
         * Converts the given object to a BSON Document. <p>
         *
         * @param type
         * @return
         */
        Document convertToBsonDocument(Object type);

        /**
         * Converts the given object to the corresponding mongo object (may or may not be a DBObject)
         *
         * @param type
         * @return
         */
        Object convert(Object type);

        Query toQuery(Object template);
    }

    /**
     * @author Elias Lindholm (elilin)
     */
    private static final class MongoConverterDocumentConverter implements DocumentConverter.Provider {

        private final MongoConverter mongoConverter;

        public MongoConverterDocumentConverter(MongoConverter mongoConverter) {
            Objects.requireNonNull(mongoConverter);
            this.mongoConverter = mongoConverter;
        }

        @Override
        public <T> T convert(Class<T> toType, Document document) {
            return mongoConverter.read(toType, document);
        }

		@Override
		public Document convertToBsonDocument(Object type) {
            Document result = new Document();
            mongoConverter.write(type, result);
            return result;
		}

		@Override
        public Object convert(Object type) {
            return mongoConverter.convertToMongoType(type);
        }

        @Override
        public Query toQuery(Object template) {
            return new MongoQueryFactory(mongoConverter).createMongoQueryFromTemplate(template);
        }

    }

}
