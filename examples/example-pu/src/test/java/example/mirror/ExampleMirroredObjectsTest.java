package example.mirror;

import java.util.Collection;

import com.avanza.ymer.MirroredObjectDefinition;
import com.avanza.ymer.YmerMirroredObjectsTestBase;

public class ExampleMirroredObjectsTest extends YmerMirroredObjectsTestBase {

	@Override
	protected Collection<MirroredObjectDefinition<?>> mirroredObjectDefinitions() {
		return ExampleMirrorFactory.getDefinitions();
	}

	@Override
	protected String basePackageForScanning() {
		return "example.domain";
	}
}
