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
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.junit.Before;
import org.junit.Test;

public class JavaInstantConverterTest {

    private JavaInstantReadConverter javaInstantReadConverter;
    private JavaInstantWriteConverter javaInstantWriteConverter;

    @Before
    public void test() {
        javaInstantReadConverter = new JavaInstantReadConverter();
        javaInstantWriteConverter = new JavaInstantWriteConverter();
    }

    @Test
    public void shouldConvertSuccessfully() {
        // Given
        Instant expected = ZonedDateTime
                .of(LocalDateTime.of(2020, 1, 15, 13, 37, 17, 123456789),
                    ZoneId.of("Europe/Stockholm"))
                .toInstant();

        // When
        Instant actual = javaInstantReadConverter.convert(javaInstantWriteConverter.convert(expected));

        // Then
        assertThat(actual, is(expected));
    }

}
