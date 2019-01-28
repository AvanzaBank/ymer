package com.avanza.ymer.support;

import org.springframework.core.convert.converter.Converter;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class JavaLocalDateReadConverter implements Converter<String, LocalDate> {

	@Override
	public LocalDate convert(String string) {
		return LocalDate.parse(string, DateTimeFormatter.ISO_LOCAL_DATE);
	}
}
