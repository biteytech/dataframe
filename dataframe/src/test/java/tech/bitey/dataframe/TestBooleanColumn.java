package tech.bitey.dataframe;

import java.util.Collection;

public class TestBooleanColumn extends TestColumn<Boolean> {

	private TestIntColumn intColumn = new TestIntColumn();
	
	@Override
	TestSample<Boolean> wrapSample(String label, Boolean[] array, int characteristics) {
		BooleanColumn column = BooleanColumn.builder().addAll(array).build();
		return new TestSample<>(label, array, 0, array.length, column);
	}
	
	@Override
	TestSample<Boolean> wrapSample(String label, Boolean[] array, Column<Boolean> column, int fromIndex, int toIndex) {
		return new TestSample<>(label, array, fromIndex, toIndex, column);
	}

	@Override public void testToSorted() {}
	@Override public void testToDistinct() {}
	
	@Override
	Boolean[] toArray(Collection<Boolean> samples) {
		return samples.toArray(empty());
	}
	
	Boolean[] fromInteger(Integer[] array) {
		Boolean[] bools = new Boolean[array.length];
		for(int i = 0; i < array.length; i++) {
			if(array[i] != null)
				bools[i] = array[i] % 2 == 0 ? Boolean.FALSE : Boolean.TRUE;
		}
		return bools;
	}
	
	@Override
	Boolean[] empty() {
		return new Boolean[0];
	}

	@Override
	Boolean[] singleNull() {
		return fromInteger(intColumn.singleNull());
	}

	@Override
	Boolean[] singleNonNull() {
		return fromInteger(intColumn.singleNull());
	}

	@Override
	Boolean[] duoFirstNull() {
		return fromInteger(intColumn.duoFirstNull());
	}

	@Override
	Boolean[] duoBothNull() {
		return fromInteger(intColumn.duoBothNull());
	}

	@Override
	Boolean[] duoDistinct() {
		return fromInteger(intColumn.duoDistinct());
	}

	@Override
	Boolean[] duoSame() {
		return fromInteger(intColumn.duoSame());
	}

	@Override
	Boolean[] minMax() {
		return fromInteger(intColumn.minMax());
	}

	@Override
	Boolean[] allNull(int size) {
		return fromInteger(intColumn.allNull(size));
	}

	@Override
	Boolean[] random(int size) {
		return fromInteger(intColumn.random(size));
	}

	@Override
	Boolean[] NXNX(int size) {
		return fromInteger(intColumn.NXNX(size));
	}

	@Override
	Boolean[] NNXX(int size) {
		return fromInteger(intColumn.NNXX(size));
	}

	@Override
	Boolean[] sequential(int size) {
		return fromInteger(intColumn.sequential(size));
	}

	@Override
	Boolean[] same(int size) {
		return fromInteger(intColumn.same(size));
	}

	@Override
	Boolean[] smar(int size) {
		return fromInteger(intColumn.smar(size));
	}
	
	@Override
	Boolean[] notPresent() {
		return new Boolean[] {};
	}
}
