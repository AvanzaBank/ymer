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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JavaInstantWriteConverterTest {

    private JavaInstantWriteConverter javaInstantWriteConverter;

    @BeforeEach
    public void setup() {
        javaInstantWriteConverter = new JavaInstantWriteConverter();
    }

    @Test
    void shouldConvertInstantIntoISOInstantValueWithNanoPrecision() {
        //Given
        String expected = "2020-07-01T13:37:17.000000470Z";
        Instant instant =
                LocalDateTime.of(2020, 7, 1, 13, 37, 17, 470)
                        .toInstant(ZoneOffset.UTC);

        // When
        String actual = javaInstantWriteConverter.convert(instant);

        // Then
        assertThat(actual, is(expected));
        assertThat(actual, is(DateTimeFormatter.ISO_INSTANT.format(instant)));
        assertThat(actual, is(instant.toString()));
    }
}