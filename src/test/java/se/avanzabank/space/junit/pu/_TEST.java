package se.avanzabank.space.junit.pu;

import org.openspaces.core.space.UrlSpaceConfigurer;

public class _TEST {
	
	public static void main(String[] args) {
		new UrlSpaceConfigurer("/./my-space").lookupGroups("kalle").create();
		new UrlSpaceConfigurer("/./my-space-2").lookupGroups("kalle").create();
		
//		new UrlSpaceConfigurer("jini://*/*/my-space-2").lookupGroups("palle").create();
		new UrlSpaceConfigurer("jini://*/*/my-space").lookupGroups("kalle").create();
		System.out.println("DONE");
		System.exit(0);
	}

}
