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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Currency;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
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
import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;
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
		MongoCustomConversions conversions = new MongoCustomConversions(Arrays.asList(
				converters
		));
		this.converter = YmerFactory.createMongoConverter(NoOpDbRefResolver.INSTANCE, conversions);
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
		assertThat(doc.get("instant"), isA(String.class));
		assertEquals(INSTANT, result.getInstant());
	}

	@Test
	void shouldHandleLocalDateTimeNanoWithCustomConverters() {
		// Arrange
		setupConverters(new JavaLocalDateTimeWriteConverter(), new JavaLocalDateTimeReadConverter());
		obj.setLocalDateTimeNano(DATE_TIME_NANO);

		// Act
		ExampleSpaceObj result = writeAndRead(obj);

		// Assert
		assertThat(doc.get("localDateTimeNano"), isA(String.class));
		assertEquals(DATE_TIME_NANO, result.getLocalDateTimeNano());
	}

	@Test
	void shouldHandleLocalDateTimeMillisWithCustomConverters() {
		// Arrange
		setupConverters(new JavaTimeLocalDateTimeWriteConverter(), new JavaTimeLocalDateTimeReadConverter());
		obj.setLocalDateTimeMillis(DATE_TIME_MILLIS);

		// Act
		ExampleSpaceObj result = writeAndRead(obj);

		// Assert
		assertThat(doc.get("localDateTimeMillis"), isA(Long.class));
		assertEquals(DATE_TIME_MILLIS, result.getLocalDateTimeMillis());
	}

	@Test
	void shouldHandleLocalDateWithCustomConverters() {
		// Arrange
		setupConverters(new JavaLocalDateWriteConverter(), new JavaLocalDateReadConverter());
		obj.setLocalDate(DATE);

		// Act
		ExampleSpaceObj result = writeAndRead(obj);

		// Assert
		assertThat(doc.get("localDate"), isA(String.class));
		assertEquals(DATE, result.getLocalDate());
	}

	@Test
	void shouldHandleCurrencyWithoutCustomConverters() {
		// Arrange
		obj.setCurrency(CURRENCY);

		// Act
		ExampleSpaceObj result = writeAndRead(obj);

		// Assert
		assertThat(doc.get("currency"), isA(String.class));
		assertEquals(CURRENCY, result.getCurrency());
	}

	@Test
	void shouldHandleYearMonthWithCustomConverters() {
		// Arrange
		setupConverters(new JavaYearMonthWriteConverter(), new JavaYearMonthReadConverter());
		obj.setYearMonth(YEAR_MONTH);

		// Act
		ExampleSpaceObj result = writeAndRead(obj);

		// Assert
		assertThat(doc.get("yearMonth"), isA(String.class));
		assertEquals(YEAR_MONTH, result.getYearMonth());
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
		assertAll(
				() -> assertThat(result.getId(), is("id_1")),
				() -> assertThat(result.getFile(), is("account")),
				() -> assertThat(result.getLocalDateTimeNano(), is(DATE_TIME_NANO)),
				() -> assertThat(result.getInstant(), is(INSTANT)),
				() -> assertThat(result.getLocalDate(), is(DATE)),
				() -> assertThat(result.getCurrency(), is(CURRENCY)),
				() -> assertThat(result.getYearMonth(), is(YEAR_MONTH)),
				() -> assertThat(result.getLocalDateTimeMillis(), is(DATE_TIME_MILLIS))
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
		assertAll(
				() -> assertThat(doc.get("_id"), is("id_2")),
				() -> assertThat(doc.get("file"), is("account")),
				() -> assertThat(doc.get("localDateTimeNano"), is(DATE_TIME_NANO_STR)),
				() -> assertThat(doc.get("instant"), is(INSTANT_STR)),
				() -> assertThat(doc.get("localDate"), is(DATE_STR)),
				() -> assertThat(doc.get("currency"), is(CURRENCY_STR)),
				() -> assertThat(doc.get("yearMonth"), is(YEAR_MONTH_STR)),

				// JavaLocalDateWriteConverter will be used since it is first in the converter list.
				// This is why this assertion compares the String and not the Long.
				() -> assertThat(doc.get("localDateTimeMillis"), is(DATE_TIME_MILLIS_STR))
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
		assertAll(
				() -> assertThat(doc.get("_id"), is("id_2")),
				() -> assertThat(doc.get("file"), is("account")),

				() -> assertThat(doc.get("instant"), is(INSTANT_STR)),
				() -> assertThat(doc.get("localDate"), is(DATE_STR)),
				() -> assertThat(doc.get("currency"), is(CURRENCY_STR)),
				() -> assertThat(doc.get("yearMonth"), is(YEAR_MONTH_STR)),

				// This should be a Long since the JavaTimeLocalDateTime converters are first.
				() -> assertThat(doc.get("localDateTimeMillis"), is(DATE_TIME_MILLIS_LONG))
				// We do not verify localDateTimeNano. It will have lost precision due to the JavaTimeLocalDateTime converters.
				// () -> assertThat(doc.get("localDateTimeNano"), is(DATE_TIME_NANO_STR)),

		);
	}

	@SpaceClass
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

		@SpaceId(autoGenerate = true)
		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		@SpaceRouting
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
	}
}
