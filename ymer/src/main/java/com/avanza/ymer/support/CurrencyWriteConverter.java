package com.avanza.ymer.support;

import org.springframework.core.convert.converter.Converter;

import java.util.Currency;

public class CurrencyWriteConverter implements Converter<Currency, String> {

	@Override
	public String convert(Currency currency) {
		return currency.toString();
	}
}
