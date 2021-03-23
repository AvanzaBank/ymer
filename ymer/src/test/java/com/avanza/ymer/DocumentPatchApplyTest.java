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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import org.bson.Document;
import org.junit.Test;
import com.mongodb.BasicDBObject;

public class DocumentPatchApplyTest {
    @Test
    public void shouldHandlePatchRemovalsUsingBasicDBObject() {

        // Given
        DocumentPatch documentPatch$1 = getDocumentPatch$1();
        DocumentPatch documentPatch$2 = getDocumentPatch$2();

        BasicDBObject initialObject = new BasicDBObject();

        // When

        documentPatch$1.apply(initialObject);
        documentPatch$2.apply(initialObject);

        // Then
        assertThat(initialObject.get("theKey"), is("theValue"));
        assertThat(initialObject.get("anotherKey"), is("anotherValue"));
        assertThat(initialObject.get("theKeyToBeRemoved"), nullValue());
    }

    @Test
    public void shouldHandlePatchRemovalsUsingDocumentOnBasicDBObject() {

        // Given
        DocumentPatch documentPatch$1 = getDocumentPatch$1();
        DocumentPatch documentPatch$2 = getDocumentPatch$2();
        Document initialObject = new Document();

        // When
        documentPatch$1.apply(initialObject);
        documentPatch$2.apply(initialObject);

        // Then
        assertThat(initialObject.get("theKey"), is("theValue"));
        assertThat(initialObject.get("anotherKey"), is("anotherValue"));
        assertThat(initialObject.get("theKeyToBeRemoved"), nullValue());
    }

    private DocumentPatch getDocumentPatch$1() {
        return new DocumentPatch() {

            @Override
            public void apply(BasicDBObject dbObject) {
                dbObject.put("theKey", "theValue");
                dbObject.put("theKeyToBeRemoved", "theValueToBeRemoved");
            }

            @Override
            public int patchedVersion() {
                return 0;
            }
        };
    }

    private DocumentPatch getDocumentPatch$2() {
        return new DocumentPatch() {

            @Override
            public void apply(BasicDBObject dbObject) {
                dbObject.put("anotherKey", "anotherValue");
                dbObject.remove("theKeyToBeRemoved");
            }

            @Override
            public int patchedVersion() {
                return 1;
            }
        };
    }
}
