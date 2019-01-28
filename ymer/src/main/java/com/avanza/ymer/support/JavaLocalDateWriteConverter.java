package com.avanza.ymer.support;

import org.springframework.core.convert.converter.Converter;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class JavaLocalDateWriteConverter implements Converter<LocalDate, String> {

	@Override
	public String convert(LocalDate localDate) {
		return localDate.format(DateTimeFormatter.ISO_LOCAL_DATE);
	}
}
