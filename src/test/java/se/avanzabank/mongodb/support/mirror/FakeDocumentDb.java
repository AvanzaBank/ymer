package se.avanzabank.mongodb.support.mirror;

import java.util.concurrent.ConcurrentHashMap;

public class FakeDocumentDb implements DocumentDb.Provider {

	private ConcurrentHashMap<String, FakeDocumentCollection> collectionByName = new ConcurrentHashMap<>();
	
	@Override
	public DocumentCollection get(String name) {
		FakeDocumentCollection documentCollection = new FakeDocumentCollection();
		collectionByName.putIfAbsent(name, documentCollection);
		return collectionByName.get(name);
	}

	public static DocumentDb create() {
		return DocumentDb.create(new FakeDocumentDb());
	}

}
