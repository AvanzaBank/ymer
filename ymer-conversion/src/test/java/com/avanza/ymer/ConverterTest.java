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
package com.avanza.ymer;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Currency;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.bson.Document;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;

import com.avanza.ymer.support.JavaInstantReadConverter;
import com.avanza.ymer.support.JavaInstantWriteConverter;
import com.avanza.ymer.support.JavaLocalDateReadConverter;
import com.avanza.ymer.support.JavaLocalDateTimeReadConverter;
import com.avanza.ymer.support.JavaLocalDateTimeWriteConverter;
import com.avanza.ymer.support.JavaLocalDateWriteConverter;
import com.avanza.ymer.support.JavaTimeLocalDateTimeReadConverter;
import com.avanza.ymer.support.JavaTimeLocalDateTimeWriteConverter;
import com.avanza.ymer.support.JavaYearMonthReadConverter;
import com.avanza.ymer.support.JavaYearMonthWriteConverter;
import com.mongodb.BasicDBObject;

class ConverterTest {

	private static final LocalDateTime DATE_TIME_NANO = LocalDateTime.of(1999, 12, 31, 3, 4, 5, 112);
	private static final String DATE_TIME_NANO_STR = "1999-12-31T03:04:05.000000112";

	private static final LocalDateTime DATE_TIME_MILLIS = LocalDateTime.of(1999, 12, 31, 3, 4, 5, 112000000);
	private static final String DATE_TIME_MILLIS_STR = "1999-12-31T03:04:05.112";
	private static final Long DATE_TIME_MILLIS_LONG = ZonedDateTime.of(DATE_TIME_MILLIS, ZoneId.systemDefault()).toInstant().toEpochMilli();

	private static final Instant INSTANT = Instant.parse("2022-07-29T15:43:56.123456789Z");
	private static final String INSTANT_STR = "2022-07-29T15:43:56.123456789Z";

	private static final LocalDate DATE = LocalDate.of(1999, 12, 31);
	private static final String DATE_STR = "1999-12-31";

	private static final Currency CURRENCY = Currency.getInstance("SEK");
	private static final String CURRENCY_STR = "SEK";

	private static final YearMonth YEAR_MONTH = YearMonth.of(2022, 4);
	private static final String YEAR_MONTH_STR = "2022-04";

	private final BasicDBObject doc = new BasicDBObject();
	private final ExampleSpaceObj obj = new ExampleSpaceObj();
	private MongoConverter converter;

	@BeforeEach
	public void beforeEachTest() {
		setupConverters();
	}

	private void setupConverters(Converter<?, ?>... converters) {
		setupConverters(Optional.empty(), converters);
	}

	private void setupConverters(Optional<String> mapKeyReplacement, Converter<?, ?>... converters) {
		YmerConverterConfiguration conf = new YmerConverterConfiguration() {
			@Override
			public List<Converter<?, ?>> getCustomConverters() {
				return Arrays.asList(converters);
			}

			@Override
			public Optional<String> getMapKeyDotReplacement() {
				return mapKeyReplacement;
			}
		};
		this.converter = YmerConverterFactory.createMongoConverter(conf, NoOpDbRefResolver.INSTANCE);
	}

	private ExampleSpaceObj writeAndRead(ExampleSpaceObj obj) {
		converter.write(obj, doc);
		return converter.read(ExampleSpaceObj.class, doc);
	}

	@Test
	void shouldHandleInstantWithCustomConverters() {
		// Arrange
		setupConverters(new JavaInstantWriteConverter(), new JavaInstantReadConverter());
		obj.setInstant(INSTANT);

		// Act
		ExampleSpaceObj result = writeAndRead(obj);

		// Assert
		MatcherAssert.assertThat(doc.get("instant"), Matchers.isA(String.class));
		Assertions.assertEquals(INSTANT, result.getInstant());
	}

	@Test
	void shouldHandleLocalDateTimeNanoWithCustomConverters() {
		// Arrange
		setupConverters(new JavaLocalDateTimeWriteConverter(), new JavaLocalDateTimeReadConverter());
		obj.setLocalDateTimeNano(DATE_TIME_NANO);

		// Act
		ExampleSpaceObj result = writeAndRead(obj);

		// Assert
		MatcherAssert.assertThat(doc.get("localDateTimeNano"), Matchers.isA(String.class));
		Assertions.assertEquals(DATE_TIME_NANO, result.getLocalDateTimeNano());
	}

	@Test
	void shouldHandleLocalDateTimeMillisWithCustomConverters() {
		// Arrange
		setupConverters(new JavaTimeLocalDateTimeWriteConverter(), new JavaTimeLocalDateTimeReadConverter());
		obj.setLocalDateTimeMillis(DATE_TIME_MILLIS);

		// Act
		ExampleSpaceObj result = writeAndRead(obj);

		// Assert
		MatcherAssert.assertThat(doc.get("localDateTimeMillis"), Matchers.isA(Long.class));
		Assertions.assertEquals(DATE_TIME_MILLIS, result.getLocalDateTimeMillis());
	}

	@Test
	void shouldHandleLocalDateWithCustomConverters() {
		// Arrange
		setupConverters(new JavaLocalDateWriteConverter(), new JavaLocalDateReadConverter());
		obj.setLocalDate(DATE);

		// Act
		ExampleSpaceObj result = writeAndRead(obj);

		// Assert
		MatcherAssert.assertThat(doc.get("localDate"), Matchers.isA(String.class));
		Assertions.assertEquals(DATE, result.getLocalDate());
	}

	@Test
	void shouldHandleCurrencyWithoutCustomConverters() {
		// Arrange
		obj.setCurrency(CURRENCY);

		// Act
		ExampleSpaceObj result = writeAndRead(obj);

		// Assert
		MatcherAssert.assertThat(doc.get("currency"), Matchers.isA(String.class));
		Assertions.assertEquals(CURRENCY, result.getCurrency());
	}

	@Test
	void shouldHandleYearMonthWithCustomConverters() {
		// Arrange
		setupConverters(new JavaYearMonthWriteConverter(), new JavaYearMonthReadConverter());
		obj.setYearMonth(YEAR_MONTH);

		// Act
		ExampleSpaceObj result = writeAndRead(obj);

		// Assert
		MatcherAssert.assertThat(doc.get("yearMonth"), Matchers.isA(String.class));
		Assertions.assertEquals(YEAR_MONTH, result.getYearMonth());
	}

	@Test
	void shouldHandleRead() {
		// Arrange
		setupConverters(
				new JavaLocalDateTimeReadConverter(), new JavaLocalDateTimeWriteConverter(),
				new JavaTimeLocalDateTimeReadConverter(), new JavaTimeLocalDateTimeWriteConverter(),
				new JavaInstantReadConverter(), new JavaInstantWriteConverter(),
				new JavaLocalDateReadConverter(), new JavaLocalDateWriteConverter(),
				new JavaYearMonthReadConverter(), new JavaYearMonthWriteConverter()
		);
		doc.put("_id", "id_1");
		doc.put("file", "account");
		doc.put("localDateTimeNano", DATE_TIME_NANO_STR);
		doc.put("instant", INSTANT_STR);
		doc.put("localDate", DATE_STR);
		doc.put("currency", CURRENCY_STR);
		doc.put("yearMonth", YEAR_MONTH_STR);
		doc.put("localDateTimeMillis", DATE_TIME_MILLIS_LONG);

		// Act
		ExampleSpaceObj result = converter.read(ExampleSpaceObj.class, doc);

		// Assert
		Assertions.assertAll(
				() -> MatcherAssert.assertThat(result.getId(), Matchers.is("id_1")),
				() -> MatcherAssert.assertThat(result.getFile(), Matchers.is("account")),
				() -> MatcherAssert.assertThat(result.getLocalDateTimeNano(), Matchers.is(DATE_TIME_NANO)),
				() -> MatcherAssert.assertThat(result.getInstant(), Matchers.is(INSTANT)),
				() -> MatcherAssert.assertThat(result.getLocalDate(), Matchers.is(DATE)),
				() -> MatcherAssert.assertThat(result.getCurrency(), Matchers.is(CURRENCY)),
				() -> MatcherAssert.assertThat(result.getYearMonth(), Matchers.is(YEAR_MONTH)),
				() -> MatcherAssert.assertThat(result.getLocalDateTimeMillis(), Matchers.is(DATE_TIME_MILLIS))
		);
	}

	@Test
	void shouldHandleWrite() {
		// Arrange
		setupConverters(
				new JavaLocalDateTimeReadConverter(), new JavaLocalDateTimeWriteConverter(),
				new JavaTimeLocalDateTimeReadConverter(), new JavaTimeLocalDateTimeWriteConverter(), // After JavaLocalDateTime converters
				new JavaInstantReadConverter(), new JavaInstantWriteConverter(),
				new JavaLocalDateReadConverter(), new JavaLocalDateWriteConverter(),
				new JavaYearMonthReadConverter(), new JavaYearMonthWriteConverter()
		);
		obj.setId("id_2");
		obj.setFile("account");
		obj.setLocalDateTimeNano(DATE_TIME_NANO);
		obj.setInstant(INSTANT);
		obj.setLocalDate(DATE);
		obj.setCurrency(CURRENCY);
		obj.setYearMonth(YEAR_MONTH);
		obj.setLocalDateTimeMillis(DATE_TIME_MILLIS);

		// Act
		converter.write(obj, doc);

		// Assert
		Assertions.assertAll(
				() -> MatcherAssert.assertThat(doc.get("_id"), Matchers.is("id_2")),
				() -> MatcherAssert.assertThat(doc.get("file"), Matchers.is("account")),
				() -> MatcherAssert.assertThat(doc.get("localDateTimeNano"), Matchers.is(DATE_TIME_NANO_STR)),
				() -> MatcherAssert.assertThat(doc.get("instant"), Matchers.is(INSTANT_STR)),
				() -> MatcherAssert.assertThat(doc.get("localDate"), Matchers.is(DATE_STR)),
				() -> MatcherAssert.assertThat(doc.get("currency"), Matchers.is(CURRENCY_STR)),
				() -> MatcherAssert.assertThat(doc.get("yearMonth"), Matchers.is(YEAR_MONTH_STR)),

				// JavaLocalDateWriteConverter will be used since it is first in the converter list.
				// This is why this assertion compares the String and not the Long.
				() -> MatcherAssert.assertThat(doc.get("localDateTimeMillis"), Matchers.is(DATE_TIME_MILLIS_STR))
		);
	}

	@Test
	void shouldHandleWrite_withJavaTimeLocalDateTimeWriteConverter() {
		// Arrange
		setupConverters(
				new JavaTimeLocalDateTimeReadConverter(), new JavaTimeLocalDateTimeWriteConverter(), // This is what we want to come into play here
				new JavaLocalDateTimeReadConverter(), new JavaLocalDateTimeWriteConverter(), // After JavaTimeLocalDateTime converters
				new JavaInstantReadConverter(), new JavaInstantWriteConverter(),
				new JavaLocalDateReadConverter(), new JavaLocalDateWriteConverter(),
				new JavaYearMonthReadConverter(), new JavaYearMonthWriteConverter()
		);
		obj.setId("id_2");
		obj.setFile("account");
		obj.setLocalDateTimeNano(DATE_TIME_NANO);
		obj.setInstant(INSTANT);
		obj.setLocalDate(DATE);
		obj.setCurrency(CURRENCY);
		obj.setYearMonth(YEAR_MONTH);
		obj.setLocalDateTimeMillis(DATE_TIME_MILLIS);

		// Act
		BasicDBObject doc = new BasicDBObject();
		converter.write(obj, doc);

		// Assert
		Assertions.assertAll(
				() -> MatcherAssert.assertThat(doc.get("_id"), Matchers.is("id_2")),
				() -> MatcherAssert.assertThat(doc.get("file"), Matchers.is("account")),

				() -> MatcherAssert.assertThat(doc.get("instant"), Matchers.is(INSTANT_STR)),
				() -> MatcherAssert.assertThat(doc.get("localDate"), Matchers.is(DATE_STR)),
				() -> MatcherAssert.assertThat(doc.get("currency"), Matchers.is(CURRENCY_STR)),
				() -> MatcherAssert.assertThat(doc.get("yearMonth"), Matchers.is(YEAR_MONTH_STR)),

				// This should be a Long since the JavaTimeLocalDateTime converters are first.
				() -> MatcherAssert.assertThat(doc.get("localDateTimeMillis"), Matchers.is(DATE_TIME_MILLIS_LONG))
				// We do not verify localDateTimeNano. It will have lost precision due to the JavaTimeLocalDateTime converters.
				// () -> assertThat(doc.get("localDateTimeNano"), is(DATE_TIME_NANO_STR)),

		);
	}

	@Test
	public void shouldHandleMapKeyDotReplacement() {
		setupConverters(Optional.of("#"));
		obj.setMap(Map.of("key.with.dot", "value.with.dot", "key_no_dot", "value_no_dot"));

		// Act
		BasicDBObject doc = new BasicDBObject();
		converter.write(obj, doc);

		// Assert
		Document mapDoc = (Document) doc.get("map");
		Assertions.assertAll(
				() -> MatcherAssert.assertThat(mapDoc.size(), Matchers.is(2)),
				() -> MatcherAssert.assertThat(mapDoc.get("key#with#dot"), Matchers.is("value.with.dot")),
				() -> MatcherAssert.assertThat(mapDoc.get("key_no_dot"), Matchers.is("value_no_dot")));
	}

	@Test
	public void shouldThrowExceptionOnMissingMapKeyDotReplacement() {
		setupConverters();
		obj.setMap(Map.of("key.with.dot", "value.with.dot", "key_no_dot", "value_no_dot"));

		BasicDBObject doc = new BasicDBObject();
		Assertions.assertThrows(MappingException.class, () -> converter.write(obj, doc));
	}

	public static class ExampleSpaceObj {

		@Id
		private String id;
		private String file;
		private LocalDateTime localDateTimeNano;
		private Instant instant;
		private LocalDate localDate;
		private Currency currency;
		private YearMonth yearMonth;
		private LocalDateTime localDateTimeMillis;
		private Map<String, String> map;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getFile() {
			return file;
		}

		public void setFile(String file) {
			this.file = file;
		}

		public LocalDateTime getLocalDateTimeNano() {
			return localDateTimeNano;
		}

		public void setLocalDateTimeNano(LocalDateTime localDateTimeNano) {
			this.localDateTimeNano = localDateTimeNano;
		}

		public Instant getInstant() {
			return instant;
		}

		public void setInstant(Instant instant) {
			this.instant = instant;
		}

		public LocalDate getLocalDate() {
			return localDate;
		}

		public void setLocalDate(LocalDate localDate) {
			this.localDate = localDate;
		}

		@Override
		public String toString() {
			return "SpaceFilePublicationEvent{" +
					"id='" + id + '\'' +
					", file='" + file + '\'' +
					", localDateTimeNano='" + localDateTimeNano + '\'' +
					", instant='" + instant + '\'' +
					", localDate='" + localDate + '\'' +
					", currency='" + currency + '\'' +
					", yearMonth='" + yearMonth + '\'' +
					", localDateTimeMillis='" + localDateTimeMillis + '\'' +
					'}';
		}

		public Currency getCurrency() {
			return currency;
		}

		public void setCurrency(Currency currency) {
			this.currency = currency;
		}

		public YearMonth getYearMonth() {
			return yearMonth;
		}

		public void setYearMonth(YearMonth yearMonth) {
			this.yearMonth = yearMonth;
		}

		public LocalDateTime getLocalDateTimeMillis() {
			return localDateTimeMillis;
		}

		public void setLocalDateTimeMillis(LocalDateTime localDateTimeMillis) {
			this.localDateTimeMillis = localDateTimeMillis;
		}

		public Map<String, String> getMap() {
			return map;
		}

		public void setMap(Map<String, String> map) {
			this.map = map;
		}
	}
}
