package se.avanzabank.space.junit.pu;

import java.io.IOException;
import java.util.Properties;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.properties.BeanLevelProperties;
import org.openspaces.pu.container.ProcessingUnitContainer;
import org.openspaces.pu.container.integrated.IntegratedProcessingUnitContainer;
import org.openspaces.pu.container.integrated.IntegratedProcessingUnitContainerProvider;
import org.springframework.context.ApplicationContext;

public class MirrorPu implements PuRunner {

	private IntegratedProcessingUnitContainer container;
	private String gigaSpaceBeanName = "gigaSpace";
	private String puXmlPath;
	private Properties contextProperties = new Properties();
	private String lookupGroupName;
	private boolean autostart;
	private ApplicationContext parentContext;
	
	public MirrorPu(MirrorPuConfigurer config) {
		this.puXmlPath = config.puXmlPath;
		this.contextProperties = config.properties;
		this.lookupGroupName = config.lookupGroupName;
		this.autostart = true;
		this.parentContext = config.parentContext;
	}

	@Override
	public void run() throws IOException {
		try {
			startContainers();
		} catch (Exception e) {
			throw new RuntimeException("Failed to start containers for puXmlPath: " + puXmlPath, e);
		}
	}

	private void startContainers() throws IOException {
		IntegratedProcessingUnitContainerProvider provider = new IntegratedProcessingUnitContainerProvider();
		provider.setBeanLevelProperties(createBeanLevelProperties());
		provider.addConfigLocation(puXmlPath);
		if (parentContext != null) {
			provider.setParentContext(parentContext);
		}
		container = (IntegratedProcessingUnitContainer) provider.createContainer();
	}

	private BeanLevelProperties createBeanLevelProperties() {
		BeanLevelProperties beanLevelProperties = new BeanLevelProperties();
		beanLevelProperties.setContextProperties(contextProperties);
		beanLevelProperties.getBeanProperties("space").put("gs.space.url.arg.timeout", "10");
		return beanLevelProperties;
	}
	
	@Override
	public void shutdown() {
		container.close();
	}

	@Override
	public String getLookupGroupName() {
		return this.lookupGroupName;
	}
	
	public boolean autostart() {
		return this.autostart;
	}
	
	@Override
	public GigaSpace getClusteredGigaSpace() {
		return GigaSpace.class.cast(container.getApplicationContext().getBean(this.gigaSpaceBeanName)).getClustered();
	}
	
	@Override
	public ApplicationContext getPrimaryInstanceApplicationContext(int partition) {
		return container.getApplicationContext();
	}


}
