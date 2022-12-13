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

import java.time.YearMonth;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;

/**
 * Converter that converts from {@link YearMonth} to a {@link String}.
 */
@WritingConverter
public class JavaYearMonthWriteConverter implements Converter<YearMonth, String> {
	@Override
	public String convert(YearMonth yearMonth) {
		return yearMonth.toString();
	}
}
