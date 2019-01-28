package com.avanza.ymer.support;

import org.springframework.core.convert.converter.Converter;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class JavaLocalDateTimeReadConverter implements Converter<String, LocalDateTime> {

	@Override
	public LocalDateTime convert(String string) {
		return LocalDateTime.parse(string, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
	}
}
