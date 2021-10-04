package tech.bitey.bufferstuff;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.ObjIntConsumer;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestBufferBitSet {

	private static final TreeSet<Integer> SAMPLE_INDICES = new TreeSet<>(
			Arrays.asList(1, 10, 38, 39, 40, 41, 42, 100, 9000));

	private static void populateWithSampleIndices(BufferBitSet bs) {
		for (int index : SAMPLE_INDICES)
			bs.set(index);
	}

	@Test
	public void basicGetAndSet() {
		Map<BufferBitSet, Boolean> setsAndIfThrows = new IdentityHashMap<>();
		setsAndIfThrows.put(new BufferBitSet(), false);
		setsAndIfThrows.put(new BufferBitSet(false), true);
		setsAndIfThrows.put(new BufferBitSet(ByteBuffer.allocate(0)), true);
		setsAndIfThrows.put(new BufferBitSet(ByteBuffer.allocate(0), false), true);
		setsAndIfThrows.put(new BufferBitSet(ByteBuffer.allocate(2000), false), false);
		setsAndIfThrows.put(new BufferBitSet().resizable(false), true);
		setsAndIfThrows.put(BufferBitSet.valueOf(new byte[0]), false);
		setsAndIfThrows.put(BufferBitSet.valueOf(new byte[] { 0 }), false);

		for (Map.Entry<BufferBitSet, Boolean> e : setsAndIfThrows.entrySet()) {
			try {
				basicGetAndSet(e.getKey());
				if (e.getValue())
					throw new RuntimeException("Expected IndexOutOfBoundsException");
			} catch (IndexOutOfBoundsException ex) {
				if (!e.getValue())
					throw ex;
			}
		}
	}

	private void basicGetAndSet(BufferBitSet bs) {

		populateWithSampleIndices(bs);
		Assertions.assertEquals(SAMPLE_INDICES.toString(), bs.toString());

		for (int i = 0; i < 20000; i++)
			Assertions.assertEquals(SAMPLE_INDICES.contains(i), bs.get(i));
	}

	@Test
	public void toFromByteArray() {

		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		BufferBitSet rebuilt = BufferBitSet.valueOf(bs.toByteArray());
		Assertions.assertEquals(SAMPLE_INDICES.toString(), rebuilt.toString());
	}

	@Test
	public void toFromBitSet() {

		BitSet bs = new BitSet();
		for (int i : SAMPLE_INDICES)
			bs.set(i);

		BufferBitSet bbs = BufferBitSet.valueOf(bs);
		Assertions.assertEquals(SAMPLE_INDICES.toString(), bbs.toString());

		BitSet rebuilt = bbs.toBitSet();
		Assertions.assertEquals(bs, rebuilt);
	}

	private void basicFlipOrClear(ObjIntConsumer<BufferBitSet> op) {

		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		Set<Integer> expected = new TreeSet<>(SAMPLE_INDICES);

		expected.remove(9000);
		op.accept(bs, 9000);

		expected.remove(38);
		op.accept(bs, 38);

		Assertions.assertEquals(expected.toString(), bs.toString());
	}

	@Test
	public void basicFlip() {
		basicFlipOrClear(BufferBitSet::flip);
	}

	@Test
	public void basicClear() {
		basicFlipOrClear(BufferBitSet::clear);

		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);
		bs.clear(Integer.MAX_VALUE);
		Assertions.assertEquals(SAMPLE_INDICES.toString(), bs.toString());
	}

	@Test
	public void setRange() {
		setRange(0, 0);
		setRange(1, 1);
		setRange(1, 2);
		setRange(0, 7);
		setRange(0, 9);
		setRange(8, 16);
		setRange(5, 20);
		setRange(1000, 2000);
	}

	private void setRange(int fromIndex, int toIndex) {

		BufferBitSet bs = new BufferBitSet();
		bs.set(fromIndex, toIndex);

		Set<Integer> expected = new TreeSet<>();
		for (int i = fromIndex; i < toIndex; i++)
			expected.add(i);

		Assertions.assertEquals(expected.toString(), bs.toString());
	}

	@Test
	public void clearRange() {
		clearRange(0, 0);
		clearRange(1, 1);
		clearRange(1, 2);
		clearRange(0, 7);
		clearRange(0, 9);
		clearRange(8, 16);
		clearRange(5, 20);
		clearRange(1000, 2000);

		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);
		bs.set(10000, 20000);
		BufferBitSet copy = bs.copy();
		copy.clear(30000, 40000);
		Assertions.assertEquals(bs, copy);
		BufferBitSet bs2 = new BufferBitSet();
		populateWithSampleIndices(bs2);
		bs.clear(10000, 20000);
		Assertions.assertEquals(bs2, bs);
	}

	private void clearRange(int fromIndex, int toIndex) {

		BufferBitSet bs = new BufferBitSet();
		bs.set(0, toIndex + 1);
		bs.clear(fromIndex, toIndex);

		Set<Integer> expected = new TreeSet<>();
		for (int i = 0; i < fromIndex; i++)
			expected.add(i);
		expected.add(toIndex);

		Assertions.assertEquals(expected.toString(), bs.toString());
	}

	@Test
	public void flipRange() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		bs.flip(0, 10000);

		Set<Integer> expected = new TreeSet<>();
		for (int i = 0; i < 10000; i++)
			if (!SAMPLE_INDICES.contains(i))
				expected.add(i);

		Assertions.assertEquals(expected.toString(), bs.toString());
	}

	@Test
	public void getRange() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		Assertions.assertEquals("[]", bs.get(0, 0).toString());
		Assertions.assertEquals("[]", bs.get(10000, 20000).toString());
		Assertions.assertEquals("[0, 28, 29]", bs.get(10, 40).toString());
		Assertions.assertEquals(SAMPLE_INDICES.toString(), bs.get(0, 9001).toString());
		Assertions.assertEquals(SAMPLE_INDICES.toString(), bs.get(0, 10000).toString());

		for (int shift = 1; shift <= 100; shift++) {
			final int s = shift;
			Set<Integer> expected = new TreeSet<>(
					SAMPLE_INDICES.stream().map(i -> i - s).filter(i -> i >= 0).collect(Collectors.toSet()));

			Assertions.assertEquals(expected.toString(), bs.get(s, 9001).toString());
		}
	}

	@Test
	public void readWrite() throws IOException {

		// test writeTo(WritableByteChannel, int, int)
		readWrite(new BufferBitSet(), 0, 0);

		BufferBitSet test = new BufferBitSet();
		test.set(0, 16);
		readWrite(test, 5, 10);

		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		readWrite(bs, 0, 0);
		readWrite(bs, 10000, 20000);
		readWrite(bs, 10, 40);
		readWrite(bs, 0, 9001);
		readWrite(bs, 0, 10000);

		for (int shift = 1; shift <= 100; shift++)
			readWrite(bs, shift, 9001);

		BufferBitSet test2 = new BufferBitSet();
		test.set(0, 128);
		for (int shift = 1; shift <= 100; shift++) {
			readWrite(test2, shift, 128);
			readWrite(test2, 0, 128 - shift);
		}

		// test writeTo(WritableByteChannel)
		File file = File.createTempFile("readWrite", "dat");
		file.deleteOnExit();

		try (FileChannel fileChannel = FileChannel.open(file.toPath(), CREATE, WRITE);) {
			bs.writeTo(fileChannel);
		}
		try (FileChannel fileChannel = FileChannel.open(file.toPath(), READ);) {
			BufferBitSet actual = BufferBitSet.readFrom(fileChannel);
			Assertions.assertEquals(bs, actual);
		}
	}

	private int readWriteNext = 0;

	public void readWrite(BufferBitSet bbs, int fromIndex, int toIndex) throws IOException {

		File file = File.createTempFile("readWrite" + readWriteNext++, "dat");
		file.deleteOnExit();

		try (FileChannel fileChannel = FileChannel.open(file.toPath(), CREATE, WRITE);) {
			bbs.writeTo(fileChannel, fromIndex, toIndex);
		}
		try (FileChannel fileChannel = FileChannel.open(file.toPath(), READ);) {
			BufferBitSet actual = BufferBitSet.readFrom(fileChannel);
			Assertions.assertEquals(bbs.get(fromIndex, toIndex), actual);
		}
	}

	@Test
	public void nextSetBit() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		Set<Integer> actual = new TreeSet<>();
		for (int bit = bs.nextSetBit(0); bit != -1; bit = bs.nextSetBit(bit + 1))
			actual.add(bit);

		Assertions.assertEquals(SAMPLE_INDICES, actual);

		Assertions.assertEquals(-1, bs.nextSetBit(10000));
	}

	@Test
	public void nextClearBit() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);
		bs.flip(0, 10000);

		Set<Integer> actual = new TreeSet<>();
		for (int bit = bs.nextClearBit(0); bit <= 9000; bit = bs.nextClearBit(bit + 1))
			actual.add(bit);

		Assertions.assertEquals(SAMPLE_INDICES, actual);

		Assertions.assertEquals(20000, bs.nextClearBit(20000));
	}

	@Test
	public void previousSetBit() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		Set<Integer> actual = new TreeSet<>();
		for (int bit = bs.previousSetBit(10000); bit != -1; bit = bs.previousSetBit(bit - 1))
			actual.add(bit);

		Assertions.assertEquals(SAMPLE_INDICES, actual);

		Assertions.assertEquals(-1, bs.previousSetBit(0));
	}

	@Test
	public void previousClearBit() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);
		bs.flip(0, 10000);

		Set<Integer> actual = new TreeSet<>();
		for (int bit = SAMPLE_INDICES.last(); bit != -1; bit = bs.previousClearBit(bit - 1))
			actual.add(bit);

		Assertions.assertEquals(SAMPLE_INDICES, actual);

		Assertions.assertEquals(-1, bs.previousClearBit(0));
	}

	@Test
	public void and() {
		BufferBitSet bs1 = new BufferBitSet();
		populateWithSampleIndices(bs1);
		bs1.clear(1);

		BufferBitSet bs2 = new BufferBitSet();
		populateWithSampleIndices(bs2);
		bs2.clear(9000);

		Set<Integer> expected = new TreeSet<>(SAMPLE_INDICES);
		expected.remove(1);
		expected.remove(9000);

		bs1.and(bs2);
		Assertions.assertEquals(expected.toString(), bs1.toString());

		bs1.and(bs1);
		Assertions.assertEquals(expected.toString(), bs1.toString());
	}

	@Test
	public void or() {
		BufferBitSet bs1 = new BufferBitSet();
		populateWithSampleIndices(bs1);
		bs1.clear(1);

		BufferBitSet bs2 = new BufferBitSet();
		populateWithSampleIndices(bs2);
		bs2.clear(9000);
		bs2.set(20000);

		Set<Integer> expected = new TreeSet<>(SAMPLE_INDICES);
		expected.add(20000);

		bs1.or(bs2);
		Assertions.assertEquals(expected.toString(), bs1.toString());

		bs1.or(bs1);
		Assertions.assertEquals(expected.toString(), bs1.toString());
	}

	@Test
	public void xor() {
		BufferBitSet bs1 = new BufferBitSet();
		populateWithSampleIndices(bs1);

		BufferBitSet bs2 = new BufferBitSet();
		populateWithSampleIndices(bs2);
		bs2.set(20000);

		Set<Integer> expected = new TreeSet<>();
		expected.add(20000);

		bs1.xor(bs2);
		Assertions.assertEquals(expected.toString(), bs1.toString());

		bs1.xor(bs1);
		Assertions.assertEquals(0, bs1.lastSetBit() + 1);
	}

	@Test
	public void andNot() {
		BufferBitSet bs1 = new BufferBitSet();
		populateWithSampleIndices(bs1);

		BufferBitSet bs2 = new BufferBitSet();
		bs2.set(20000);

		bs1.andNot(bs2);
		Assertions.assertEquals(SAMPLE_INDICES.toString(), bs1.toString());

		bs2.set(9000);
		bs1.andNot(bs2);
		Set<Integer> expected = new TreeSet<>(SAMPLE_INDICES);
		expected.remove(9000);
		Assertions.assertEquals(expected.toString(), bs1.toString());
	}

	@Test
	public void lastSetBit() {
		BufferBitSet bs = new BufferBitSet();
		Assertions.assertEquals(-1, bs.lastSetBit());

		bs.set(0);
		Assertions.assertEquals(0, bs.lastSetBit());

		populateWithSampleIndices(bs);
		Assertions.assertEquals(SAMPLE_INDICES.last(), bs.lastSetBit());

		bs.set(Integer.MAX_VALUE);
		Assertions.assertEquals(Integer.MAX_VALUE, bs.lastSetBit());
	}

	@Test
	public void size() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		bs = bs.resizable(false);

		bs.set(bs.size() - 1);
		try {
			bs.set(bs.size());
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}
	}

	@Test
	public void isEmpty() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		bs.xor(bs);
		Assertions.assertTrue(bs.isEmpty());

		Assertions.assertTrue(new BufferBitSet().isEmpty());
		Assertions.assertTrue(BufferBitSet.valueOf(new byte[0]).isEmpty());
		Assertions.assertTrue(BufferBitSet.valueOf(new byte[] { 0 }).isEmpty());
		Assertions.assertTrue(new BufferBitSet(ByteBuffer.allocate(0)).isEmpty());
		Assertions.assertTrue(new BufferBitSet(ByteBuffer.allocate(1)).isEmpty());
	}

	@Test
	public void cardinality() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		Assertions.assertEquals(SAMPLE_INDICES.size(), bs.cardinality());
		Assertions.assertEquals(SAMPLE_INDICES.size(), bs.cardinality(0, 10000));
		Assertions.assertEquals(0, new BufferBitSet().cardinality());
		Assertions.assertEquals(0, new BufferBitSet().cardinality(0, 10));

		BufferBitSet bs2 = new BufferBitSet();
		bs2.set(0, 24);
		for (int i = 0; i <= 24; i++)
			Assertions.assertEquals(i, bs2.cardinality(0, i));
		Assertions.assertEquals(3, bs2.cardinality(3, 6));
		Assertions.assertEquals(3, bs2.cardinality(6, 9));
	}

	@Test
	public void hashCodeTest() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		int expected = Arrays.hashCode(bs.getBuffer().array());

		Assertions.assertEquals(expected, bs.hashCode());
		Assertions.assertEquals(0, new BufferBitSet().hashCode());
	}

	@Test
	public void equals() {
		Assertions.assertFalse(new BufferBitSet().equals(null));
		Assertions.assertTrue(new BufferBitSet().equals(new BufferBitSet()));

		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		Assertions.assertTrue(bs.equals(bs));
		Assertions.assertFalse(new BufferBitSet().equals(bs));
		Assertions.assertFalse(bs.equals(new BufferBitSet()));

		BufferBitSet bs2 = new BufferBitSet();
		populateWithSampleIndices(bs2);
		Assertions.assertTrue(bs.equals(bs2));

		bs2.set(0);
		Assertions.assertFalse(bs.equals(bs2));
		Assertions.assertFalse(bs2.equals(bs));
	}

	@Test
	public void emptyToString() {
		Assertions.assertEquals("[]", new BufferBitSet().toString());
	}

	@Test
	public void shiftRight() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		for (int shift = 0; shift <= 100; shift++) {
			final int s = shift;
			Set<Integer> expected = new TreeSet<>(SAMPLE_INDICES.stream().map(i -> i + s).collect(Collectors.toSet()));

			Assertions.assertEquals(expected.toString(), bs.shiftRight(shift).toString());
		}
	}

	@Test
	public void testClone() {
		BufferBitSet bs1 = new BufferBitSet();
		populateWithSampleIndices(bs1);

		BufferBitSet clone1 = (BufferBitSet) bs1.clone();
		Assertions.assertEquals(bs1, clone1);

		BufferBitSet bs2 = new BufferBitSet();
		populateWithSampleIndices(bs2);
		bs2.set(9010);

		BufferBitSet clone2 = (BufferBitSet) bs2.clone();
		Assertions.assertEquals(bs2, clone2);
	}

	@Test
	public void testRandom() {

		for (int r = 0; r < 4; r++) {
			for (int size = 0; size <= 100; size++) {
				for (int n = 0; n <= size; n++) {

					Boolean[] b = new Boolean[size];
					Arrays.fill(b, Boolean.FALSE);
					Arrays.fill(b, 0, n, Boolean.TRUE);
					Collections.shuffle(Arrays.asList(b), new Random(r * r * r));
					BufferBitSet expected = new BufferBitSet();
					for (int i = 0; i < b.length; i++)
						if (b[i])
							expected.set(i);

					BufferBitSet actual = BufferBitSet.random(n, size, new Random(r * r * r));

					Assertions.assertEquals(expected, actual);
				}
			}
		}
	}

	@Test
	public void badIndices() {

		BufferBitSet bs = new BufferBitSet();

		try {
			bs.get(-1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.set(-1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.flip(-1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.clear(-1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.set(-1, 0);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.set(0, -1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.set(7, 3);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.clear(-1, 0);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.clear(0, -1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.clear(7, 3);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.nextSetBit(-1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.nextClearBit(-1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.previousSetBit(-2);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.previousClearBit(-2);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.shiftRight(-1);
			throw new RuntimeException("Expected IllegalArgumentException");
		} catch (IllegalArgumentException ex) {
			// good
		}
	}
}
