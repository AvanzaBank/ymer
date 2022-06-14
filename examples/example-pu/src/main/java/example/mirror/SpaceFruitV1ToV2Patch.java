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
package example.mirror;

import org.bson.Document;

import com.avanza.ymer.BsonDocumentPatch;

public class SpaceFruitV1ToV2Patch implements BsonDocumentPatch {

	@Override
	public void apply(Document document) {
		document.put("organic", false);
	}

	@Override
	public int patchedVersion() {
		return 1;
	}

}
