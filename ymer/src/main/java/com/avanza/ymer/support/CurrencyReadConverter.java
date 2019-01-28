package com.avanza.ymer.support;

import org.springframework.core.convert.converter.Converter;

import java.util.Currency;

public class CurrencyReadConverter implements Converter<String, Currency> {

	@Override
	public Currency convert(String string) {
		return Currency.getInstance(string);
	}
}
