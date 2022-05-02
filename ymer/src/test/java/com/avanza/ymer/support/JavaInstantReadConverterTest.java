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
package com.avanza.ymer.support;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JavaInstantReadConverterTest {

    private JavaInstantReadConverter javaInstantReadConverter;

    @BeforeEach
    public void setup() {
        javaInstantReadConverter = new JavaInstantReadConverter();
    }

    @Test
    void shouldConvertISOInstantValueIntoInstantWithNanoPrecision() {
        // Given
        Instant expected =
                LocalDateTime
                .of(2020, 7, 29, 15, 43, 56, 863000000)
                .toInstant(ZoneOffset.UTC);

        // When
        Instant actual = javaInstantReadConverter.convert("2020-07-29T15:43:56.863Z");

        // Then
        assertNotNull(actual);
        assertThat(actual.getEpochSecond(), is(expected.getEpochSecond()));
        assertThat(actual.getNano(), is(expected.getNano()));
    }
}