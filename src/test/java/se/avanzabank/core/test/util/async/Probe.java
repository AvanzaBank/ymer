package se.avanzabank.core.test.util.async;

import org.hamcrest.Description;

/**
 * Used in combination with a Poller to test asynchronous code. More specifically to 
 * wait for a given condition to be satisfied by polling a given resource. <p>
 * 
 * See Poller for usage.<p>
 * 
 * 
 * @author Elias Lindholm
 *
 */
public interface Probe {
	
	/**
	 * Indicates whether this Probe is satisfied or note, will be called repeatedly
	 * until timeout occurs. Timeout is determine by the Poller that polls the probe. <p>
	 * 
	 * @return
	 */
	boolean isSatisfied();
	
	/**
	 * Sample the given resource. After each sample the isSatisfied() method will be queried
	 * to see whether to condition i yet satisfied. <p> 
	 */
	void sample();
	
	/**
	 * This method will be invoked in case of failure, i.e. the probe was not satisfied before
	 * timeout occurs. <p>
	 * 
	 * @param description
	 */
	void describeFailureTo(Description description);

}
