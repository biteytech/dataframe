package tech.bitey.dataframe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class TestByteColumn extends TestColumn<Byte> {

	private static final Random RAND = new Random(0);

	@Override
	TestSample<Byte> wrapSample(String label, Byte[] array, int characteristics) {
		ByteColumn column = ByteColumn.builder(characteristics).addAll(array).build();
		return new TestSample<>(label, array, 0, array.length, column);
	}

	@Override
	TestSample<Byte> wrapSample(String label, Byte[] array, Column<Byte> column, int fromIndex, int toIndex) {
		return new TestSample<>(label, array, fromIndex, toIndex, column);
	}

	@Override
	Byte[] toArray(Collection<Byte> samples) {
		return samples.toArray(empty());
	}

	@Override
	Byte[] empty() {
		return new Byte[0];
	}

	@Override
	Byte[] singleNull() {
		return new Byte[] { null };
	}

	@Override
	Byte[] singleNonNull() {
		return new Byte[] { 0 };
	}

	@Override
	Byte[] duoFirstNull() {
		return new Byte[] { null, 0 };
	}

	@Override
	Byte[] duoBothNull() {
		return new Byte[] { null, null };
	}

	@Override
	Byte[] duoDistinct() {
		return new Byte[] { 0, 1 };
	}

	@Override
	Byte[] duoSame() {
		return new Byte[] { 0, 0 };
	}

	@Override
	Byte[] minMax() {
		return new Byte[] { Byte.MIN_VALUE, Byte.MAX_VALUE };
	}

	@Override
	Byte[] allNull(int size) {
		return new Byte[size];
	}

	@Override
	Byte[] random(int size) {
		List<Byte> list = new ArrayList<>(Arrays.asList(RANDOM));
		Collections.shuffle(list, RAND);
		return list.subList(0, size).toArray(new Byte[0]);
	}

	@Override
	Byte[] NXNX(int size) {
		Byte[] random = random(size);
		for (int i = 0; i < size; i += 2)
			random[i] = null;
		return random;
	}

	@Override
	Byte[] NNXX(int size) {
		Byte[] random = random(size);
		for (int i = 0; i < size; i += 4) {
			random[i] = null;
			if (i + 1 < size)
				random[i + 1] = null;
		}
		return random;
	}

	@Override
	Byte[] sequential(int size) {
		Byte[] elements = new Byte[size];
		for (int i = 0; i < size; i++)
			elements[i] = (byte) i;
		return elements;
	}

	@Override
	Byte[] same(int size) {
		Byte[] elements = new Byte[size];
		for (int i = 0; i < size; i++)
			elements[i] = 0;
		return elements;
	}

	@Override
	Byte[] smar(int size) {
		Byte[] elements = new Byte[size];
		byte n = 1;
		for (int i = 1; i < elements.length; n++)
			for (int j = 0; j < n && i < elements.length; j++)
				elements[i++] = n;
		return elements;
	}

	@Override
	Byte[] notPresent() {
		return new Byte[] { -77, -88, -99 };
	}

	// 1026 values
	private static final Byte[] RANDOM = new Byte[1026];
	static {
		for (int i = 0; i < 1024; i++)
			RANDOM[i] = Byte.valueOf((byte) (i & 0x7F));
		RANDOM[1024] = Byte.MIN_VALUE;
		RANDOM[1025] = Byte.MIN_VALUE;
	}
}