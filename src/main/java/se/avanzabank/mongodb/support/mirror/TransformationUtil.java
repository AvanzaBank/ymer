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
package se.avanzabank.mongodb.support.mirror;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class TransformationUtil {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TransformationUtil.class);

	// ----- Internal helper classes -----

	private interface TransformationUnit {
		void apply(DBObject target);
	}
	public interface EndpointTransformation {
		void apply(DBObject dbObject, Path endingPath);
	}

	public static class Path {
		private static final Pattern PATH_PATTERN = Pattern.compile("^[^.\\s]+(\\.[^.\\s]*)*$");
		private final List<String> path;
		public Path(String path) {
			if (!PATH_PATTERN.matcher(path).find()) {
				throw new IllegalArgumentException("Bad path: " + path);
			}
			this.path = Arrays.asList(path.split("\\."));
		}
		private Path(List<String> path) {
			this.path = path;
		}
		public String head() {
			return path.get(0);
		}
		public boolean isEnding() {
			return path.size() == 1;
		}
		public Path tail() {
			return new Path(path.subList(1, path.size()));
		}

		public void follow(DBObject current, EndpointTransformation et) {
			if (isEnding()) {
				et.apply(current, this);
			} else {
				Object next = current.get(head());
				if (next instanceof BasicDBList) {
					for (Object o : (BasicDBList) next) {
						tail().follow((DBObject) o, et);
					}
				} else if (next != null) {
					tail().follow((DBObject) next, et);
				}
			}
		}

		// Does not support paths that traverse over (a) list element(s)
		public Object get(DBObject current) {
			if (current == null) {
				return null;
			}

			if (isEnding()) {
				return current.get(head());
			} else {
				Object next = current.get(head());
				if (!(next instanceof DBObject)) {
					throw new RuntimeException("Cannot follow mongo object of type " + current.getClass().getName() + " at path " + this);
				}
				return tail().get((DBObject) next);
			}
		}

		@Override
		public String toString() {
			return StringUtils.collectionToDelimitedString(path, ".");
		}
	}

	private static class EnsureFieldRemoved implements TransformationUnit, EndpointTransformation {
		private final Path path;

		public EnsureFieldRemoved(Path path) {
			Objects.requireNonNull(path);
			this.path = path;
		}

		@Override
		public void apply(DBObject target) {
			//ensureFieldRemoved(target, path);
			path.follow(target, this);
		}

		@Override
		public void apply(DBObject current, Path endingPath) {
			current.removeField(endingPath.head());
		}

		@Override
		public String toString() {
			return "EnsureFieldRemoved [path=" + path + "]";
		}
	}

	private static class ConvertValue implements TransformationUnit, EndpointTransformation {
		private final Path path;
		private final Object oldValue;
		private final Object convertedValue;

		public ConvertValue(Path path, Object oldValue, Object convertedValue) {
			Objects.requireNonNull(path);
			Objects.requireNonNull(oldValue);
			Objects.requireNonNull(convertedValue);
			this.path = path;
			this.oldValue = oldValue;
			this.convertedValue = convertedValue;
		}

		@Override
		public void apply(DBObject target) {
			path.follow(target, this);
		}

		@Override
		public void apply(DBObject dbObject, Path endingPath) {
			if (dbObject.get(endingPath.head()).equals(oldValue)) {
				dbObject.put(endingPath.head(), convertedValue);
			}
		}

		@Override
		public String toString() {
			return "ConvertValue [path=" + path + ", oldValue=" + oldValue + ", convertedValue=" + convertedValue + "]";
		}
	}

	private static final class EnsurePathExists implements TransformationUnit {
		private final Path path;

		public EnsurePathExists(Path path) {
			this.path = Objects.requireNonNull(path);
		}

		@Override
		public void apply(DBObject target) {
			Path p = path;
			while (!p.isEnding()) {
				Object current = target.get(p.head());
				if (current == null) {
					current = new BasicDBObject();
					target.put(p.head(), current);
				} else if (!(current instanceof DBObject)) {
					throw new RuntimeException("Cannot follow mongo object of type " + current.getClass().getName());
				}
				target = (DBObject) current;
				p = p.tail();
			}
		}

	}

	public static class DBObjectMatcher {
		private static final Object ANYTHING = new Object();
		private final Map<String, Object> requiredFields = new HashMap<>();
		public boolean matches(DBObject dbObject) {
			for (Map.Entry<String, Object> requiredField : requiredFields.entrySet()) {
				final Object requiredValue = requiredField.getValue();
				if (requiredValue == null && !matchNullValue(requiredField.getKey(), dbObject)) {
					return false;
				} else if (requiredValue == ANYTHING) {
					if (!containsKey(requiredField.getKey(), dbObject)) {
						return false;
					}
				} else if (requiredValue != null && !matchValue(dbObject, requiredField, requiredValue)) {
					return false;
				}
			}
			return true;
		}
		private boolean containsKey(String key, DBObject dbObject) {
			return dbObject.containsField(key);
		}
		
		private boolean matchNullValue(String key, DBObject dbObject) {
			if (!dbObject.containsField(key)) {
				return false;
			}
			final Object obj = dbObject.get(key);
			return (obj instanceof DBObject)
					&& ((DBObject) obj).keySet().isEmpty();
		}
		private boolean matchValue(DBObject dbObject, Map.Entry<String, Object> requiredField, final Object requiredValue) {
			if (!requiredValue.equals(dbObject.get(requiredField.getKey()))) {
				return false;
			}
			return true;
		}
		public DBObjectMatcher hasField(String fieldName, Object value) {
			this.requiredFields.put(fieldName, value);
			return this;
		}
		
		/**
		 * Enforces the matcher to match only objects that contains the given field. Value will not be matched.
		 * @param fieldName The field name to match
		 * @return this (for fluent building)
		 */
		public DBObjectMatcher hasField(String fieldName) {
			this.requiredFields.put(fieldName, ANYTHING);
			return this;
		}
		
	}

	private static final class AddValue implements TransformationUnit, EndpointTransformation {
		private final Path targetPath;
		private final Object value;
		private final DBObjectMatcher matcher;

		public AddValue(Path targetPath, Object value, DBObjectMatcher matcher) {
			this.targetPath = targetPath;
			this.value = value;
			this.matcher = matcher;
		}

		@Override
		public void apply(DBObject target) {
			targetPath.follow(target, this);
		}

		@Override
		public void apply(DBObject dbObject, Path endingPath) {
			if (dbObject != null
					&& (matcher == null || matcher.matches(dbObject))) {
				dbObject.put(endingPath.head(), value);
			}
		}
		
		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder("AddValue [");
			builder.append("targetPath=").append(targetPath);
			builder.append(", value=").append(value);
			builder.append("]");
			return builder.toString();
		}

	}

	private static final class SetValue implements TransformationUnit, EndpointTransformation {
		private final Path path;
		private final Object value;

		public SetValue(Path path, Object value) {
			this.path = Objects.requireNonNull(path);
			this.value = value;
		}

		@Override
		public void apply(DBObject target) {
			new EnsurePathExists(path).apply(target);
			path.follow(target, this);
		}

		@Override
		public void apply(DBObject dbObject, Path endingPath) {
			dbObject.put(endingPath.head(), value);
		}
	}

	private static final class MoveValue implements TransformationUnit {
		private final Path sourcePath;
		private final Path targetPath;

		public MoveValue(Path sourcePath, Path targetPath) {
			this.sourcePath = Objects.requireNonNull(sourcePath);
			this.targetPath = Objects.requireNonNull(targetPath);
		}

		@Override
		public void apply(DBObject target) {
			final Object data = sourcePath.get(target);
			if (data != null) {
				new EnsurePathExists(targetPath).apply(target);
				targetPath.follow(target, new EndpointTransformation() {
					@Override
					public void apply(DBObject dbObject, Path endingPath) {
						dbObject.put(endingPath.head(), data);
					}
				});
				new EnsureFieldRemoved(sourcePath).apply(target);
			}
		}
		
		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder("MoveValue [");
			builder.append("sourcePath=").append(sourcePath);
			builder.append(", targetPath=").append(targetPath);
			builder.append("]");
			return builder.toString();
		}
	}

	public interface Transformer {
		Object transform(Object o);
	}

	private static final class Transform implements TransformationUnit, EndpointTransformation {
		private final Path path;
		private final Transformer transformer;

		public Transform(Path path, Transformer transformer) {
			this.path = Objects.requireNonNull(path);
			this.transformer = Objects.requireNonNull(transformer);
		}

		@Override
		public void apply(DBObject target) {
			path.follow(target, this);
		}

		@Override
		public void apply(DBObject dbObject, Path endingPath) {
			Object object = dbObject.get(endingPath.head());
			dbObject.put(endingPath.head(), transformer.transform(object));
		}
	}

	// ----- Class implementation -----

	private final List<TransformationUnit> transformations = new ArrayList<>();

	public DBObject apply(DBObject target) {
		for (TransformationUnit tu : transformations) {
			try {
				tu.apply(target);
			} catch (RuntimeException e) {
				LOGGER.error("Exception occured, transformation was not completed. transformationUnit=" + tu + " target=" + target);
				throw e;
			}
		}
		return target;
	}

	public TransformationUtil ensureRemoved(String path) {
		return register(new EnsureFieldRemoved(new Path(path)));
	}

	public TransformationUtil ensurePathExists(String path) {
		return register(new EnsurePathExists(new Path(path)));
	}

	public TransformationUtil convertValue(String path, Object oldValue, Object convertedValue) {
		return register(new ConvertValue(new Path(path), oldValue, convertedValue));
	}

	public TransformationUtil move(String sourcePath, String targetPath) {
		return register(new MoveValue(new Path(sourcePath), new Path(targetPath)));
	}

	public TransformationUtil setValue(String path, Object value) {
		return register(new SetValue(new Path(path), value));
	}

	public TransformationUtil addValue(String path, Object value, DBObjectMatcher matcher) {
		return register(new AddValue(new Path(path), value, matcher));
	}

	public TransformationUtil transform(String path, Transformer transformer) {
		return register(new Transform(new Path(path), transformer));
	}

	// ----- private helper methods -----

	private TransformationUtil register(TransformationUnit tu) {
		transformations.add(tu);
		return this;
	}

	@Override
	public String toString() {
		return "TransformationUtil [transformations=" + transformations + "]";
	}

}
