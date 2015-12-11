package se.avanzabank.mongodb.support;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class IteratorIteratorTest {

	@Test
	public void joinsSeveralIteratorsIntoOne() throws Exception {
		List<Object> list1 = Arrays.<Object>asList(1, 2, 3, 4);
		List<Object> list2 = Arrays.<Object>asList("apa", "banan", "müsli");
		List<Object> list3 = Collections.emptyList();
		List<Object> list4 = Arrays.<Object>asList(9, 10);

		IteratorIterator<Object> ii = new IteratorIterator<Object>(list1.iterator(), list2.iterator(), list3.iterator(), list4.iterator());

		assertEquals(1, ii.next());
		assertEquals(2, ii.next());
		assertEquals(3, ii.next());
		assertEquals(4, ii.next());
		assertEquals("apa", ii.next());
		assertEquals("banan", ii.next());
		assertEquals("müsli", ii.next());
		assertEquals(9, ii.next());
		assertEquals(10, ii.next());
	}

}
