package tech.bitey.bufferstuff;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestBufferSort {

	private final int[] isorted = { 1, 2, 3 };
	private final int[] ireverse = { 3, 2, 1 };

	private final int[] irandom = { -311, -509, -74, -128, -695, 859, 852, -888, -149, -431, 589, -354, 71, -110, 236,
			74, 976, -653, -80, 420, -340, -686, -275, 740, 265, -937, 118, -948, 667, -743, -194, 186, -498, -830, 995,
			-847, -334, 922, -521, -786, -179, 117, -971, -823, 593, -235, 344, -827, -246, 324, -662, -489, 153, 969,
			-593, -214, 75, -643, 26, -188, 2, 640, -799, -231, 299, -927, -870, 473, 388, -96, -505, -891, 423, -660,
			140, 64, -364, -636, 280, 930, 701, 278, 180, 554, 113, 910, -883, 924, 986, 374, 4, 616, 443, 444, 261,
			-843, -25, -252, -837, -43, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE };

	@Test
	public void sortInt() {

		sort(new int[] {}, 0, 0);
		sort(new int[] { 0 }, 0, 1);
		sort(new int[] { 0, 0 }, 0, 2);
		sort(isorted, 0, 3);
		sort(ireverse, 0, 3);

		for (int l = 2; l <= irandom.length; l++) {
			for (int fromIndex = 0;; fromIndex++) {
				int toIndex = fromIndex + l;
				if (toIndex > irandom.length)
					break;
				sort(irandom, fromIndex, toIndex);
			}
		}
	}

	@FunctionalInterface
	private interface IntBufferSort {
		void sort(IntBuffer b, int fromIndex, int toIndex);
	}

	public void sort(int[] array, int fromIndex, int toIndex) {

		int[] expected = Arrays.copyOf(array, array.length);
		Arrays.sort(expected, fromIndex, toIndex);

		for (IntBufferSort sort : new IntBufferSort[] { BufferSort::insertionSort, BufferSort::heapSort,
				BufferSort::radixSort, BufferSort::sort }) {
			IntBuffer actual = IntBuffer.wrap(Arrays.copyOf(array, array.length));
			sort.sort(actual, fromIndex, toIndex);

			Assertions.assertArrayEquals(Arrays.copyOfRange(expected, fromIndex, toIndex),
					Arrays.copyOfRange(actual.array(), fromIndex, toIndex));
		}
	}

	// =============================================================================================

	private final long[] lsorted = { 1, 2, 3 };
	private final long[] lreverse = { 3, 2, 1 };

	private final long[] lrandom = { -311, -509, -74, -128, -695, 859, 852, -888, -149, -431, 589, -354, 71, -110, 236,
			74, 976, -653, -80, 420, -340, -686, -275, 740, 265, -937, 118, -948, 667, -743, -194, 186, -498, -830, 995,
			-847, -334, 922, -521, -786, -179, 117, -971, -823, 593, -235, 344, -827, -246, 324, -662, -489, 153, 969,
			-593, -214, 75, -643, 26, -188, 2, 640, -799, -231, 299, -927, -870, 473, 388, -96, -505, -891, 423, -660,
			140, 64, -364, -636, 280, 930, 701, 278, 180, 554, 113, 910, -883, 924, 986, 374, 4, 616, 443, 444, 261,
			-843, -25, -252, -837, -43, Long.MAX_VALUE, Long.MAX_VALUE, Long.MIN_VALUE, Long.MIN_VALUE };

	@Test
	public void sortLong() {

		sort(new long[] {}, 0, 0);
		sort(new long[] { 0 }, 0, 1);
		sort(new long[] { 0, 0 }, 0, 2);
		sort(lsorted, 0, 3);
		sort(lreverse, 0, 3);

		for (int l = 2; l <= lrandom.length; l++) {
			for (int fromIndex = 0;; fromIndex++) {
				int toIndex = fromIndex + l;
				if (toIndex > lrandom.length)
					break;
				sort(lrandom, fromIndex, toIndex);
			}
		}
	}

	@FunctionalInterface
	private interface LongBufferSort {
		void sort(LongBuffer b, int fromIndex, int toIndex);
	}

	public void sort(long[] array, int fromIndex, int toIndex) {

		long[] expected = Arrays.copyOf(array, array.length);
		Arrays.sort(expected, fromIndex, toIndex);

		for (LongBufferSort sort : new LongBufferSort[] { BufferSort::insertionSort, BufferSort::heapSort,
				BufferSort::radixSort, BufferSort::sort }) {
			LongBuffer actual = LongBuffer.wrap(Arrays.copyOf(array, array.length));
			sort.sort(actual, fromIndex, toIndex);

			Assertions.assertArrayEquals(Arrays.copyOfRange(expected, fromIndex, toIndex),
					Arrays.copyOfRange(actual.array(), fromIndex, toIndex));
		}
	}

	// =============================================================================================

	private final short[] ssorted = { 1, 2, 3 };
	private final short[] sreverse = { 3, 2, 1 };

	private final short[] srandom = { -311, -509, -74, -128, -695, 859, 852, -888, -149, -431, 589, -354, 71, -110, 236,
			74, 976, -653, -80, 420, -340, -686, -275, 740, 265, -937, 118, -948, 667, -743, -194, 186, -498, -830, 995,
			-847, -334, 922, -521, -786, -179, 117, -971, -823, 593, -235, 344, -827, -246, 324, -662, -489, 153, 969,
			-593, -214, 75, -643, 26, -188, 2, 640, -799, -231, 299, -927, -870, 473, 388, -96, -505, -891, 423, -660,
			140, 64, -364, -636, 280, 930, 701, 278, 180, 554, 113, 910, -883, 924, 986, 374, 4, 616, 443, 444, 261,
			-843, -25, -252, -837, -43, Short.MAX_VALUE, Short.MAX_VALUE, Short.MIN_VALUE, Short.MIN_VALUE };

	@Test
	public void sortShort() {

		sort(new short[] {}, 0, 0);
		sort(new short[] { 0 }, 0, 1);
		sort(new short[] { 0, 0 }, 0, 2);
		sort(ssorted, 0, 3);
		sort(sreverse, 0, 3);

		for (int l = 2; l <= srandom.length; l++) {
			for (int fromIndex = 0;; fromIndex++) {
				int toIndex = fromIndex + l;
				if (toIndex > srandom.length)
					break;
				sort(srandom, fromIndex, toIndex);
			}
		}
	}

	@FunctionalInterface
	private interface ShortBufferSort {
		void sort(ShortBuffer b, int fromIndex, int toIndex);
	}

	public void sort(short[] array, int fromIndex, int toIndex) {

		short[] expected = Arrays.copyOf(array, array.length);
		Arrays.sort(expected, fromIndex, toIndex);

		for (ShortBufferSort sort : new ShortBufferSort[] { BufferSort::insertionSort, BufferSort::heapSort,
				BufferSort::countingSort, BufferSort::sort }) {
			ShortBuffer actual = ShortBuffer.wrap(Arrays.copyOf(array, array.length));
			sort.sort(actual, fromIndex, toIndex);

			Assertions.assertArrayEquals(Arrays.copyOfRange(expected, fromIndex, toIndex),
					Arrays.copyOfRange(actual.array(), fromIndex, toIndex));
		}
	}

	// =============================================================================================

	private final byte[] bsorted = { 1, 2, 3 };
	private final byte[] breverse = { 3, 2, 1 };

	private final byte[] brandom = { -111, -109, -74, -128, -95, 59, 52, -88, -49, -31, 89, -54, 71, -110, 36, 74, 76,
			-53, -80, 120, -40, -86, -75, 40, 65, -37, 118, -48, 67, -43, -94, 86, -98, -30, 95, -47, -34, 122, -121,
			-86, -79, 117, -71, -123, 93, -35, 44, -127, -46, 124, -62, -89, 53, 69, -93, -114, 75, -3, 26, -8, 2, 40,
			-99, -13, 99, -127, -70, 73, 18, -96, -105, -91, 123, -16, 40, 64, -16, -13, 18, 0, 101, 78, 80, 54, 113,
			110, -83, 124, 18, 74, 4, 116, 43, 44, 61, -43, -25, -52, -37, -43, Byte.MAX_VALUE, Byte.MAX_VALUE,
			Byte.MIN_VALUE, Byte.MIN_VALUE };

	@Test
	public void sortByte() {

		sort(new byte[] {}, 0, 0);
		sort(new byte[] { 0 }, 0, 1);
		sort(new byte[] { 0, 0 }, 0, 2);
		sort(bsorted, 0, 3);
		sort(breverse, 0, 3);

		for (int l = 2; l <= brandom.length; l++) {
			for (int fromIndex = 0;; fromIndex++) {
				int toIndex = fromIndex + l;
				if (toIndex > brandom.length)
					break;
				sort(brandom, fromIndex, toIndex);
			}
		}
	}

	@FunctionalInterface
	private interface ByteBufferSort {
		void sort(ByteBuffer b, int fromIndex, int toIndex);
	}

	public void sort(byte[] array, int fromIndex, int toIndex) {

		byte[] expected = Arrays.copyOf(array, array.length);
		Arrays.sort(expected, fromIndex, toIndex);

		for (ByteBufferSort sort : new ByteBufferSort[] { BufferSort::insertionSort, BufferSort::heapSort,
				BufferSort::countingSort, BufferSort::sort }) {
			ByteBuffer actual = ByteBuffer.wrap(Arrays.copyOf(array, array.length));
			sort.sort(actual, fromIndex, toIndex);

			Assertions.assertArrayEquals(Arrays.copyOfRange(expected, fromIndex, toIndex),
					Arrays.copyOfRange(actual.array(), fromIndex, toIndex));
		}
	}

	// =============================================================================================

	private final float[] fsorted = { 1, 2, 3 };
	private final float[] freverse = { 3, 2, 1 };

	private final float[] frandom = { -311, -509, -74, -128, -695, 859, 852, -888, -149, -431, 589, -354, 71, -110, 236,
			74, 976, -653, -80, 420, -340, -686, -275, 740, 265, -937, 118, -948, 667, -743, -194, 186, -498, -830, 995,
			-847, -334, 922, -521, -786, -179, 117, -971, -823, 593, -235, 344, -827, -246, 324, -662, -489, 153, 969,
			-593, -214, 75, -643, 26, -188, 2, 640, -799, -231, 299, -927, -870, 473, 388, -96, -505, -891, 423, -660,
			140, 64, -364, -636, 280, 930, 701, 278, 180, 554, 113, 910, -883, 924, 986, 374, 4, 616, 443, 444, 261,
			-843, -25, -252, -837, -43, Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY,
			Float.NEGATIVE_INFINITY };

	@Test
	public void sortFloat() {

		sort(new float[] {}, 0, 0);
		sort(new float[] { 0 }, 0, 1);
		sort(new float[] { 0, 0 }, 0, 2);
		sort(new float[] { Float.NaN, Float.POSITIVE_INFINITY, Float.NaN, Float.MAX_VALUE, 0, -0f,
				Float.NEGATIVE_INFINITY }, 0, 7);
		sort(fsorted, 0, 3);
		sort(freverse, 0, 3);

		for (int l = 2; l <= frandom.length; l++) {
			for (int fromIndex = 0;; fromIndex++) {
				int toIndex = fromIndex + l;
				if (toIndex > frandom.length)
					break;
				sort(frandom, fromIndex, toIndex);
			}
		}
	}

	@FunctionalInterface
	private interface FloatBufferSort {
		void sort(FloatBuffer b, int fromIndex, int toIndex);
	}

	public void sort(float[] array, int fromIndex, int toIndex) {

		float[] expected = Arrays.copyOf(array, array.length);
		Arrays.sort(expected, fromIndex, toIndex);

		for (FloatBufferSort sort : new FloatBufferSort[] { BufferSort::insertionSort, BufferSort::heapSort,
				BufferSort::sort }) {
			FloatBuffer actual = FloatBuffer.wrap(Arrays.copyOf(array, array.length));
			sort.sort(actual, fromIndex, toIndex);

			Assertions.assertArrayEquals(Arrays.copyOfRange(expected, fromIndex, toIndex),
					Arrays.copyOfRange(actual.array(), fromIndex, toIndex));
		}
	}

	// =============================================================================================

	private final double[] dsorted = { 1, 2, 3 };
	private final double[] dreverse = { 3, 2, 1 };

	private final double[] drandom = { -311, -509, -74, -128, -695, 859, 852, -888, -149, -431, 589, -354, 71, -110,
			236, 74, 976, -653, -80, 420, -340, -686, -275, 740, 265, -937, 118, -948, 667, -743, -194, 186, -498, -830,
			995, -847, -334, 922, -521, -786, -179, 117, -971, -823, 593, -235, 344, -827, -246, 324, -662, -489, 153,
			969, -593, -214, 75, -643, 26, -188, 2, 640, -799, -231, 299, -927, -870, 473, 388, -96, -505, -891, 423,
			-660, 140, 64, -364, -636, 280, 930, 701, 278, 180, 554, 113, 910, -883, 924, 986, 374, 4, 616, 443, 444,
			261, -843, -25, -252, -837, -43, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
			Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY };

	@Test
	public void sortDouble() {

		sort(new double[] {}, 0, 0);
		sort(new double[] { 0 }, 0, 1);
		sort(new double[] { 0, 0 }, 0, 2);
		sort(new double[] { Double.NaN, Double.POSITIVE_INFINITY, Double.NaN, Double.MAX_VALUE, 0, -0f,
				Double.NEGATIVE_INFINITY }, 0, 7);
		sort(dsorted, 0, 3);
		sort(dreverse, 0, 3);

		for (int l = 2; l <= drandom.length; l++) {
			for (int fromIndex = 0;; fromIndex++) {
				int toIndex = fromIndex + l;
				if (toIndex > drandom.length)
					break;
				sort(drandom, fromIndex, toIndex);
			}
		}
	}

	@FunctionalInterface
	private interface DoubleBufferSort {
		void sort(DoubleBuffer b, int fromIndex, int toIndex);
	}

	public void sort(double[] array, int fromIndex, int toIndex) {

		double[] expected = Arrays.copyOf(array, array.length);
		Arrays.sort(expected, fromIndex, toIndex);

		for (DoubleBufferSort sort : new DoubleBufferSort[] { BufferSort::insertionSort, BufferSort::heapSort,
				BufferSort::sort }) {
			DoubleBuffer actual = DoubleBuffer.wrap(Arrays.copyOf(array, array.length));
			sort.sort(actual, fromIndex, toIndex);

			Assertions.assertArrayEquals(Arrays.copyOfRange(expected, fromIndex, toIndex),
					Arrays.copyOfRange(actual.array(), fromIndex, toIndex));
		}
	}
}
