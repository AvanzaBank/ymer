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

import java.util.Collection;

/**
 * Manages 'on demand' reload of a mirrored object from its secondary (persistent) storage, typically
 * a Mongo database.<p>
 *
 * In order to be reloadable, the space object must implement {@link ReloadableSpaceObject}. <p>
 *
 * @author joasah Joakim Sahlström
 */
public interface SpaceObjectLoader {

	/**
	 * Triggers reload of a given space object from the secondary storage. <p>
	 *
	 * @param spaceType
	 * @param documentId
	 * @return
	 *
	 * @deprecated Use {@link SpaceObjectLoader#loadObject(Class, Object)} instead
	 */
	@Deprecated
	<T extends ReloadableSpaceObject> T reloadObject(Class<T> spaceType, Object documentId);

	<T> T loadObject(Class<T> spaceType, Object documentId);
	
	<T> Collection<T> loadObjects(Class<T> spaceType, T template);

}
