package com.avanza.ymer;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.springframework.core.convert.converter.Converter;

public interface YmerConverterConfiguration {
	
	default List<Converter<?, ?>> getCustomConverters() {
		return Collections.emptyList();
	}

	/**
	 * Defines the replacement string for map keys with a dot.
	 * Default value is {@link Optional#empty()}
	 *
	 * @return an optional value defining the replacement for
	 * @see org.springframework.data.mongodb.core.convert.MappingMongoConverter#setMapKeyDotReplacement(String)
	 */
	default Optional<String> getMapKeyDotReplacement() {
		return Optional.empty();
	}
}
