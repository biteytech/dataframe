package tech.bitey.dataframe;

import java.util.List;

abstract class TestFloatingColumn<E extends Comparable<E>> extends TestColumn<E> {

	@Override
	List<TestSample<E>> baseSamples() {
		
		List<TestSample<E>> samples = super.baseSamples();
		
		samples.add(wrapSample("singleNaN", singleNaN()));		
		samples.add(wrapSample("duoNaN", duoNaN()));	
		samples.add(wrapSample("nonFinite", nonFinite()));
		
		return samples;
	}
	
	abstract E[] singleNaN();
	abstract E[] duoNaN();
	abstract E[] nonFinite();
}
