package se.avanzabank.mongodb.support.mirror;

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.stream.StreamSupport;

/**
 * Created by Daniel Bergholm
 */
final class Iterables {
	private Iterables(){}

	public static <T> List<T> newArrayList(Iterable<T> iterable) {
		return StreamSupport.stream(iterable.spliterator(), false).collect(toList());
	}

	public static int sizeOf(Iterable<?> iterable) {
		return (int) StreamSupport.stream(iterable.spliterator(), false).count();
	}
}
