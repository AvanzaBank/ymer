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

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Utility class for holding all DocumentPatches associated with a given type. <p>
 * 
 * @author Elias Lindholm (elilin)
 *
 * @param <T>
 */
final class DocumentPatchChain<T> implements Iterable<BsonDocumentPatch> {

	// TODO: remove mirroredType field and info from DocumentPatchChain
	
	private Class<T> mirroredType;
	private BsonDocumentPatch[] patchChain;

	DocumentPatchChain(Class<T> mirroredType, List<BsonDocumentPatch> patches) {
		this.mirroredType = mirroredType;
		this.patchChain = new BsonDocumentPatch[patches.size()];
		sortPatchChain(patches);
		for (int i = 0; i < patches.size(); i++) {
			patchChain[i] = patches.get(i);
			if (i == 0) {
				continue;
			}
			BsonDocumentPatch previousPatchInChain = patchChain[i - 1];
			BsonDocumentPatch currentPatchInChain = patchChain[i];
			if (previousPatchInChain.patchedVersion() == currentPatchInChain.patchedVersion()) {
				throw new IllegalArgumentException(String.format("Chain of patches must not contain duplicates of patches for: [%s]", currentPatchInChain));
			}
			if (previousPatchInChain.patchedVersion() + 1 != currentPatchInChain.patchedVersion()) {
				throw new IllegalArgumentException(String.format("Chain of patches must be continuous with no holes, hole found between patches for version [%s] and [%s]", previousPatchInChain.patchedVersion(), currentPatchInChain.patchedVersion()));
			}
		}
	}
	
	private void sortPatchChain(List<BsonDocumentPatch> patches) {
		patches.sort(Comparator.comparingInt(BsonDocumentPatch::patchedVersion));
	}
	
	Class<T> getMirroredType() {
		return mirroredType;
	}

	@Override
	public Iterator<BsonDocumentPatch> iterator() {
		return Arrays.asList(this.patchChain).iterator();
	}
	
	boolean isEmpty() {
		return this.patchChain.length == 0;
	}

	BsonDocumentPatch getLastPatchInChain() {
		return this.patchChain[this.patchChain.length - 1];
	}

	BsonDocumentPatch getFirstPatchInChain() {
		return this.patchChain[0];
	}

	/**
	 * Returns the patch for a given version. <p>
	 * 
	 * @param version
	 * @return
	 */
	BsonDocumentPatch getPatch(int version) {
		if (version > getLastPatchInChain().patchedVersion() || version < getFirstPatchInChain().patchedVersion()) {
			throw new IllegalArgumentException("No such patch: " + version);
		}
		return this.patchChain[version - getFirstPatchInChain().patchedVersion()];
	}

}
