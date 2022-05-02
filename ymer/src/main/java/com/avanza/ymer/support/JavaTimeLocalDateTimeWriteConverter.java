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

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;

/**
 * Converter that converts from a {@link LocalDateTime} to a Long. This is a good choice if
 * millisecond time precision is sufficient. Otherwise {@link JavaLocalDateTimeWriteConverter}
 * could be a better choice.
 */
@WritingConverter
public class JavaTimeLocalDateTimeWriteConverter implements Converter<LocalDateTime, Long> {

    @Override
    public Long convert(LocalDateTime localDateTime) {
        ZonedDateTime atZone = localDateTime.atZone(ZoneId.systemDefault());
        return atZone.toInstant().toEpochMilli();
    }
}
