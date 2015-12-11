package se.avanzabank.space.junit.pu;

import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.springframework.context.ApplicationContext;

public class MirrorPuConfigurer {
	
	final String puXmlPath;
	Properties properties;
	ApplicationContext parentContext;
	String lookupGroupName = JVMGlobalLus.getLookupGroupName();

	public MirrorPuConfigurer(String puXmlPath) {
		this.puXmlPath = puXmlPath;
	}

	public MirrorPuConfigurer contextProperties(Properties properties) {
		this.properties = properties;
		return this;
	}

	public MirrorPuConfigurer parentContext(ApplicationContext parentContext) {
		this.parentContext = parentContext;
		return this;
	}

	public RunningPu configure() {
		return new RunningPuImpl(new MirrorPu(this));
	}

}
