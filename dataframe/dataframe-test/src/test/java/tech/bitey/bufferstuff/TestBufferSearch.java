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

public class TestBufferSearch {

	private static final int IMIN = Integer.MIN_VALUE;
	private static final int IMAX = Integer.MAX_VALUE;

	private static final int[][][] BINARY_SEARCH_INT = {
			// {fromIndex, toIndex}, {array to search}, {values to search for}
			{ { 0, 0 }, {}, { IMIN, -1, 0, 1, 2, IMAX } }, { { 0, 1 }, { 0 }, { IMIN, -1, 0, 1, 2, IMAX } },
			{ { 0, 2 }, { IMIN, IMAX }, { IMIN, -1, 0, 1, 2, IMAX } },
			{ { 0, 3 }, { IMIN, 0, IMAX }, { IMIN, -1, 0, 1, 2, IMAX } },
			{ { 0, 5 }, { 1, 2, 3, 4, 5 }, { 0, 1, 2, 3, 4, 5, 6 } },
			{ { 0, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 1, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 2, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 3, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 4, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 0, 3 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 0, 2 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 0, 1 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } }, };

	@Test
	public void binarySearchInt() {

		for (int[][] config : BINARY_SEARCH_INT) {

			final int fromIndex = config[0][0];
			final int toIndex = config[0][1];
			final int[] a = config[1];
			final IntBuffer b = IntBuffer.wrap(a);

			for (int value : config[2]) {

				int expected = Arrays.binarySearch(a, fromIndex, toIndex, value);
				int actual = BufferSearch.binarySearch(b, fromIndex, toIndex, value);
				Assertions.assertEquals(expected, actual);
			}
		}
	}

	// ================================================================================================

	private static final long LMIN = Long.MIN_VALUE;
	private static final long LMAX = Long.MAX_VALUE;

	private static final long[][][] BINARY_SEARCH_LONG = {
			// {fromIndex, toIndex}, {array to search}, {values to search for}
			{ { 0, 0 }, {}, { LMIN, -1, 0, 1, 2, LMAX } }, { { 0, 1 }, { 0 }, { LMIN, -1, 0, 1, 2, LMAX } },
			{ { 0, 2 }, { LMIN, LMAX }, { LMIN, -1, 0, 1, 2, LMAX } },
			{ { 0, 3 }, { LMIN, 0, LMAX }, { LMIN, -1, 0, 1, 2, LMAX } },
			{ { 0, 5 }, { 1, 2, 3, 4, 5 }, { 0, 1, 2, 3, 4, 5, 6 } },
			{ { 0, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 1, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 2, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 3, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 4, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 0, 3 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 0, 2 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 0, 1 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } }, };

	@Test
	public void binarySearchLong() {

		for (long[][] config : BINARY_SEARCH_LONG) {

			final int fromIndex = (int) config[0][0];
			final int toIndex = (int) config[0][1];
			final long[] a = config[1];
			final LongBuffer b = LongBuffer.wrap(a);

			for (long value : config[2]) {

				int expected = Arrays.binarySearch(a, fromIndex, toIndex, value);
				int actual = BufferSearch.binarySearch(b, fromIndex, toIndex, value);
				Assertions.assertEquals(expected, actual);
			}
		}
	}

	// ================================================================================================

	private static final short SMIN = Short.MIN_VALUE;
	private static final short SMAX = Short.MAX_VALUE;

	private static final short[][][] BINARY_SEARCH_SHORT = {
			// {fromIndex, toIndex}, {array to search}, {values to search for}
			{ { 0, 0 }, {}, { SMIN, -1, 0, 1, 2, SMAX } }, { { 0, 1 }, { 0 }, { SMIN, -1, 0, 1, 2, SMAX } },
			{ { 0, 2 }, { SMIN, SMAX }, { SMIN, -1, 0, 1, 2, SMAX } },
			{ { 0, 3 }, { SMIN, 0, SMAX }, { SMIN, -1, 0, 1, 2, SMAX } },
			{ { 0, 5 }, { 1, 2, 3, 4, 5 }, { 0, 1, 2, 3, 4, 5, 6 } },
			{ { 0, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 1, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 2, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 3, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 4, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 0, 3 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 0, 2 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 0, 1 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } }, };

	@Test
	public void binarySearchShort() {

		for (short[][] config : BINARY_SEARCH_SHORT) {

			final int fromIndex = config[0][0];
			final int toIndex = config[0][1];
			final short[] a = config[1];
			final ShortBuffer b = ShortBuffer.wrap(a);

			for (short value : config[2]) {

				int expected = Arrays.binarySearch(a, fromIndex, toIndex, value);
				int actual = BufferSearch.binarySearch(b, fromIndex, toIndex, value);
				Assertions.assertEquals(expected, actual);
			}
		}
	}

	// ================================================================================================

	private static final byte BMIN = Byte.MIN_VALUE;
	private static final byte BMAX = Byte.MAX_VALUE;

	private static final byte[][][] BINARY_SEARCH_BYTE = {
			// {fromIndex, toIndex}, {array to search}, {values to search for}
			{ { 0, 0 }, {}, { BMIN, -1, 0, 1, 2, BMAX } }, { { 0, 1 }, { 0 }, { BMIN, -1, 0, 1, 2, BMAX } },
			{ { 0, 2 }, { BMIN, BMAX }, { BMIN, -1, 0, 1, 2, BMAX } },
			{ { 0, 3 }, { BMIN, 0, BMAX }, { BMIN, -1, 0, 1, 2, BMAX } },
			{ { 0, 5 }, { 1, 2, 3, 4, 5 }, { 0, 1, 2, 3, 4, 5, 6 } },
			{ { 0, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 1, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 2, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 3, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 4, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 0, 3 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 0, 2 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } },
			{ { 0, 1 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4 } }, };

	@Test
	public void binarySearchByte() {

		for (byte[][] config : BINARY_SEARCH_BYTE) {

			final int fromIndex = config[0][0];
			final int toIndex = config[0][1];
			final byte[] a = config[1];
			final ByteBuffer b = ByteBuffer.wrap(a);

			for (byte value : config[2]) {

				int expected = Arrays.binarySearch(a, fromIndex, toIndex, value);
				int actual = BufferSearch.binarySearch(b, fromIndex, toIndex, value);
				Assertions.assertEquals(expected, actual);
			}
		}
	}

	// ================================================================================================

	private static final float FMIN = Float.NEGATIVE_INFINITY;
	private static final float FMAX = Float.POSITIVE_INFINITY;
	private static final float FNAN = Float.NaN;

	private static final float[][][] BINARY_SEARCH_FLOAT = {
			// {fromIndex, toIndex}, {array to search}, {values to search for}
			{ { 0, 0 }, {}, { FMIN, -1, 0, 1, 2, FMAX, FNAN } },
			{ { 0, 1 }, { 0, FNAN }, { FMIN, -1, 0, 1, 2, FMAX, FNAN } },
			{ { 0, 2 }, { FMIN, FMAX }, { FMIN, -1, 0, 1, 2, FMAX, FNAN } },
			{ { 0, 3 }, { FMIN, 0, FMAX, FNAN }, { FMIN, -1, 0, 1, 2, FMAX, FNAN } },
			{ { 0, 5 }, { 1, 2, 3, 4, 5 }, { 0, 1, 2, 3, 4, 5, 6, FNAN } },
			{ { 0, 4 }, { -3, -1, 1, 3, FNAN }, { -4, -3, -2, -1, 0, 1, 2, 3, 4, FNAN } },
			{ { 1, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4, FNAN } },
			{ { 2, 4 }, { -3, -1, 1, 3, FNAN }, { -4, -3, -2, -1, 0, 1, 2, 3, 4, FNAN } },
			{ { 3, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4, FNAN } },
			{ { 4, 4 }, { -3, -1, 1, 3, FNAN }, { -4, -3, -2, -1, 0, 1, 2, 3, 4, FNAN } },
			{ { 0, 3 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4, FNAN } },
			{ { 0, 2 }, { -3, -1, 1, 3, FNAN }, { -4, -3, -2, -1, 0, 1, 2, 3, 4, FNAN } },
			{ { 0, 1 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4, FNAN } }, };

	@Test
	public void binarySearchFloat() {

		for (float[][] config : BINARY_SEARCH_FLOAT) {

			final int fromIndex = (int) config[0][0];
			final int toIndex = (int) config[0][1];
			final float[] a = config[1];
			final FloatBuffer b = FloatBuffer.wrap(a);

			for (float value : config[2]) {

				int expected = Arrays.binarySearch(a, fromIndex, toIndex, value);
				int actual = BufferSearch.binarySearch(b, fromIndex, toIndex, value);
				Assertions.assertEquals(expected, actual);
			}
		}
	}

	// ================================================================================================

	private static final double DMIN = Double.NEGATIVE_INFINITY;
	private static final double DMAX = Double.POSITIVE_INFINITY;
	private static final double DNAN = Double.NaN;

	private static final double[][][] BINARY_SEARCH_DOUBLE = {
			// {fromIndex, toIndex}, {array to search}, {values to search for}
			{ { 0, 0 }, {}, { DMIN, -1, 0, 1, 2, DMAX, DNAN } },
			{ { 0, 1 }, { 0, DNAN }, { DMIN, -1, 0, 1, 2, DMAX, DNAN } },
			{ { 0, 2 }, { DMIN, DMAX }, { DMIN, -1, 0, 1, 2, DMAX, DNAN } },
			{ { 0, 3 }, { DMIN, 0, DMAX, DNAN }, { DMIN, -1, 0, 1, 2, DMAX, DNAN } },
			{ { 0, 5 }, { 1, 2, 3, 4, 5 }, { 0, 1, 2, 3, 4, 5, 6, DNAN } },
			{ { 0, 4 }, { -3, -1, 1, 3, DNAN }, { -4, -3, -2, -1, 0, 1, 2, 3, 4, DNAN } },
			{ { 1, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4, DNAN } },
			{ { 2, 4 }, { -3, -1, 1, 3, DNAN }, { -4, -3, -2, -1, 0, 1, 2, 3, 4, DNAN } },
			{ { 3, 4 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4, DNAN } },
			{ { 4, 4 }, { -3, -1, 1, 3, DNAN }, { -4, -3, -2, -1, 0, 1, 2, 3, 4, DNAN } },
			{ { 0, 3 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4, DNAN } },
			{ { 0, 2 }, { -3, -1, 1, 3, DNAN }, { -4, -3, -2, -1, 0, 1, 2, 3, 4, DNAN } },
			{ { 0, 1 }, { -3, -1, 1, 3 }, { -4, -3, -2, -1, 0, 1, 2, 3, 4, DNAN } }, };

	@Test
	public void binarySearchDouble() {

		for (double[][] config : BINARY_SEARCH_DOUBLE) {

			final int fromIndex = (int) config[0][0];
			final int toIndex = (int) config[0][1];
			final double[] a = config[1];
			final DoubleBuffer b = DoubleBuffer.wrap(a);

			for (double value : config[2]) {

				int expected = Arrays.binarySearch(a, fromIndex, toIndex, value);
				int actual = BufferSearch.binarySearch(b, fromIndex, toIndex, value);
				Assertions.assertEquals(expected, actual);
			}
		}
	}

	// ================================================================================================

	@Test
	public void binaryFindFirstInt() {

		for (int i = 0; i < 100; i++) {
			for (int j = 1; j < 100; j++) {

				int[] array = new int[i + j];
				Arrays.fill(array, 0, i, 888);
				Arrays.fill(array, i, i + j, 999);

				Assertions.assertEquals(i, BufferSearch.binaryFindFirst(IntBuffer.wrap(array), 0, array.length - 1));
			}
		}
	}

	@Test
	public void binaryFindLastInt() {

		for (int i = 1; i < 100; i++) {
			for (int j = 0; j < 100; j++) {

				int[] array = new int[i + j];
				Arrays.fill(array, 0, i, 888);
				Arrays.fill(array, i, i + j, 999);

				Assertions.assertEquals(i - 1, BufferSearch.binaryFindLast(IntBuffer.wrap(array), array.length, 0));
			}
		}
	}

	// ================================================================================================

	@Test
	public void binaryFindFirstLong() {

		for (int i = 0; i < 100; i++) {
			for (int j = 1; j < 100; j++) {

				long[] array = new long[i + j];
				Arrays.fill(array, 0, i, 888);
				Arrays.fill(array, i, i + j, 999);

				Assertions.assertEquals(i, BufferSearch.binaryFindFirst(LongBuffer.wrap(array), 0, array.length - 1));
			}
		}
	}

	@Test
	public void binaryFindLastLong() {

		for (int i = 1; i < 100; i++) {
			for (int j = 0; j < 100; j++) {

				long[] array = new long[i + j];
				Arrays.fill(array, 0, i, 888);
				Arrays.fill(array, i, i + j, 999);

				Assertions.assertEquals(i - 1, BufferSearch.binaryFindLast(LongBuffer.wrap(array), array.length, 0));
			}
		}
	}

	// ================================================================================================

	@Test
	public void binaryFindFirstShort() {

		for (int i = 0; i < 100; i++) {
			for (int j = 1; j < 100; j++) {

				short[] array = new short[i + j];
				Arrays.fill(array, 0, i, (short) 888);
				Arrays.fill(array, i, i + j, (short) 999);

				Assertions.assertEquals(i, BufferSearch.binaryFindFirst(ShortBuffer.wrap(array), 0, array.length - 1));
			}
		}
	}

	@Test
	public void binaryFindLastShort() {

		for (int i = 1; i < 100; i++) {
			for (int j = 0; j < 100; j++) {

				short[] array = new short[i + j];
				Arrays.fill(array, 0, i, (short) 888);
				Arrays.fill(array, i, i + j, (short) 999);

				Assertions.assertEquals(i - 1, BufferSearch.binaryFindLast(ShortBuffer.wrap(array), array.length, 0));
			}
		}
	}

	// ================================================================================================

	@Test
	public void binaryFindFirstByte() {

		for (int i = 0; i < 100; i++) {
			for (int j = 1; j < 100; j++) {

				byte[] array = new byte[i + j];
				Arrays.fill(array, 0, i, (byte) 88);
				Arrays.fill(array, i, i + j, (byte) 99);

				Assertions.assertEquals(i, BufferSearch.binaryFindFirst(ByteBuffer.wrap(array), 0, array.length - 1));
			}
		}
	}

	@Test
	public void binaryFindLastByte() {

		for (int i = 1; i < 100; i++) {
			for (int j = 0; j < 100; j++) {

				byte[] array = new byte[i + j];
				Arrays.fill(array, 0, i, (byte) 88);
				Arrays.fill(array, i, i + j, (byte) 99);

				Assertions.assertEquals(i - 1, BufferSearch.binaryFindLast(ByteBuffer.wrap(array), array.length, 0));
			}
		}
	}

	// ================================================================================================

	@Test
	public void binaryFindFirstFloat() {

		for (int i = 0; i < 100; i++) {
			for (int j = 1; j < 100; j++) {

				float[] array = new float[i + j];
				Arrays.fill(array, 0, i, 888);
				Arrays.fill(array, i, i + j, 999);

				Assertions.assertEquals(i, BufferSearch.binaryFindFirst(FloatBuffer.wrap(array), 0, array.length - 1));
			}
		}

	}

	@Test
	public void binaryFindLastFloat() {

		for (int i = 1; i < 100; i++) {
			for (int j = 0; j < 100; j++) {

				float[] array = new float[i + j];
				Arrays.fill(array, 0, i, 888);
				Arrays.fill(array, i, i + j, 999);

				Assertions.assertEquals(i - 1, BufferSearch.binaryFindLast(FloatBuffer.wrap(array), array.length, 0));
			}
		}
	}

	// ================================================================================================

	@Test
	public void binaryFindFirstDouble() {

		for (int i = 0; i < 100; i++) {
			for (int j = 1; j < 100; j++) {

				double[] array = new double[i + j];
				Arrays.fill(array, 0, i, 888);
				Arrays.fill(array, i, i + j, 999);

				Assertions.assertEquals(i, BufferSearch.binaryFindFirst(DoubleBuffer.wrap(array), 0, array.length - 1));
			}
		}

	}

	@Test
	public void binaryFindLastDouble() {

		for (int i = 1; i < 100; i++) {
			for (int j = 0; j < 100; j++) {

				double[] array = new double[i + j];
				Arrays.fill(array, 0, i, 888);
				Arrays.fill(array, i, i + j, 999);

				Assertions.assertEquals(i - 1, BufferSearch.binaryFindLast(DoubleBuffer.wrap(array), array.length, 0));
			}
		}
	}
}
