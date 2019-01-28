package com.avanza.ymer.support;

import org.springframework.core.convert.converter.Converter;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class JavaLocalDateTimeWriteConverter implements Converter<LocalDateTime, String> {

	@Override
	public String convert(LocalDateTime localDateTime) {
		return localDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
	}
}
