package com.avanza.ymer;

import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.data.convert.ConverterBuilder.ConverterAware;

/**
 * Type-safe registration of type converters.
 *
 * @see org.springframework.data.convert.CustomConversions CustomConversions
 */
public interface ConverterConfigurer {

	void add(Converter<?, ?>... converters);

	void add(ConverterFactory<?, ?>... converters);

	void add(GenericConverter... converters);

	void add(ConverterAware... converters);

}
