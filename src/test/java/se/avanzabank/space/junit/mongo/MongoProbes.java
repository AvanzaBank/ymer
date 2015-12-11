package se.avanzabank.space.junit.mongo;


import java.util.ArrayList;
import java.util.List;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.springframework.data.mongodb.core.MongoOperations;

import se.avanzabank.core.test.util.async.Probe;

/**
 * Probes to assist with asynchronous testing with Mongo.
 * 
 * @author Kristoffer Erlandsson (krierl), kristoffer.erlandsson@avanzabank.se
 */
public class MongoProbes {

	/**
	 * This probe checks all objects of the specified class in the mongo instance. If any of them matches the matcher,
	 * the probe is satisfied.
	 */
	public static <T> Probe containsObject(final MongoOperations mongo, final Matcher<T> matcher,
			final Class<T> objectClass) {
		return new Probe() {

			private List<T> sampledObjects = new ArrayList<T>();

			@Override
			public void sample() {
				sampledObjects = mongo.findAll(objectClass);
			}

			@Override
			public boolean isSatisfied() {
				for (T object : sampledObjects) {
					if (matcher.matches(object)) {
						return true;
					}
				}
				return false;
			}

			@Override
			public void describeFailureTo(Description description) {
				StringBuilder sb = new StringBuilder();
				for (T details : sampledObjects) {
					sb.append(details);
					sb.append(" ");
				}
				description.appendText("failed to find object in mongo that ").appendDescriptionOf(matcher)
						.appendText(" - most recent sampled objects: ").appendText(sb.toString().trim());
			}
		};

	}

}
