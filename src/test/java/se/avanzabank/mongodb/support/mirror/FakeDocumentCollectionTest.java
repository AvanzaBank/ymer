/**
 * 
 */
package se.avanzabank.mongodb.support.mirror;



/**
 * @author Elias Lindholm (elilin)
 *
 */
public final class FakeDocumentCollectionTest extends DocumentCollectionContract {

	/* (non-Javadoc)
	 * @see se.avanzabank.mongodb.support.mirror.DocumentCollectionContract#createEmptyCollection()
	 */
	@Override
	protected DocumentCollection createEmptyCollection() {
		return new FakeDocumentCollection();
	}

}
