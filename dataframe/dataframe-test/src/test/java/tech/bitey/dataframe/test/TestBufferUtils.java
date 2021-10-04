package tech.bitey.bufferstuff;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.ShortBuffer;
import java.util.Arrays;
import java.util.Iterator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestBufferUtils {

	private static final int IMIN = Integer.MIN_VALUE;
	private static final int IMAX = Integer.MAX_VALUE;

	private static final int[][] SORTED_INT = { {}, { 0 }, { 0, 0 }, { IMIN, IMIN }, { IMAX, IMAX }, { IMIN, IMAX },
			{ IMIN, 0, IMAX }, { -2, -1, 1, 2 }, };
	private static final int[][] NOT_SORTED_INT = { { 1, 0 }, { IMAX, IMIN }, { 3, 2, 1 }, };

	@Test
	public void isSortedInt() {
		Assertions.assertTrue(BufferUtils.isSorted(IntBuffer.allocate(0), 0, 0));
		Assertions.assertTrue(BufferUtils.isSorted(IntBuffer.allocate(1), 0, 0));
		Assertions.assertTrue(BufferUtils.isSorted(IntBuffer.allocate(1), 0, 1));

		for (int[] array : SORTED_INT) {
			IntBuffer b = IntBuffer.wrap(array);
			Assertions.assertTrue(BufferUtils.isSorted(b, 0, array.length));
			if (array.length > 0) {
				Assertions.assertTrue(BufferUtils.isSorted(b, 0, array.length - 1));
				Assertions.assertTrue(BufferUtils.isSorted(b, 1, array.length));
			}
		}

		for (int[] array : NOT_SORTED_INT)
			Assertions.assertFalse(BufferUtils.isSorted(IntBuffer.wrap(array), 0, array.length));
	}

	private static final int[][] SORTED_DISTINCT_INT = { {}, { 0 }, { IMIN, IMAX }, { IMIN, 0, IMAX },
			{ -2, -1, 1, 2 }, };
	private static final int[][] NOT_SORTED_DISTINCT_INT = { { 1, 0 }, { IMAX, IMIN }, { 3, 2, 1 }, { 0, 0 },
			{ IMIN, IMIN }, { IMAX, IMAX }, { -2, -1, -1 } };

	@Test
	public void isSortedAndDistinctInt() {
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(IntBuffer.allocate(0), 0, 0));
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(IntBuffer.allocate(1), 0, 0));
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(IntBuffer.allocate(1), 0, 1));

		for (int[] array : SORTED_DISTINCT_INT)
			Assertions.assertTrue(BufferUtils.isSortedAndDistinct(IntBuffer.wrap(array), 0, array.length));

		for (int[] array : NOT_SORTED_DISTINCT_INT)
			Assertions.assertFalse(BufferUtils.isSortedAndDistinct(IntBuffer.wrap(array), 0, array.length));
	}

	// ======================================================================================

	private static final long LMIN = Long.MIN_VALUE;
	private static final long LMAX = Long.MAX_VALUE;

	private static final long[][] SORTED_LONG = { {}, { 0 }, { 0, 0 }, { LMIN, LMIN }, { LMAX, LMAX }, { LMIN, LMAX },
			{ LMIN, 0, LMAX }, { -2, -1, 1, 2 }, };
	private static final long[][] NOT_SORTED_LONG = { { 1, 0 }, { LMAX, LMIN }, { 3, 2, 1 }, };

	@Test
	public void isSortedLong() {
		Assertions.assertTrue(BufferUtils.isSorted(LongBuffer.allocate(0), 0, 0));
		Assertions.assertTrue(BufferUtils.isSorted(LongBuffer.allocate(1), 0, 0));
		Assertions.assertTrue(BufferUtils.isSorted(LongBuffer.allocate(1), 0, 1));

		for (long[] array : SORTED_LONG) {
			LongBuffer b = LongBuffer.wrap(array);
			Assertions.assertTrue(BufferUtils.isSorted(b, 0, array.length));
			if (array.length > 0) {
				Assertions.assertTrue(BufferUtils.isSorted(b, 0, array.length - 1));
				Assertions.assertTrue(BufferUtils.isSorted(b, 1, array.length));
			}
		}

		for (long[] array : NOT_SORTED_LONG)
			Assertions.assertFalse(BufferUtils.isSorted(LongBuffer.wrap(array), 0, array.length));
	}

	private static final long[][] SORTED_DISTINCT_LONG = { {}, { 0 }, { LMIN, LMAX }, { LMIN, 0, LMAX },
			{ -2, -1, 1, 2 }, };
	private static final long[][] NOT_SORTED_DISTINCT_LONG = { { 1, 0 }, { LMAX, LMIN }, { 3, 2, 1 }, { 0, 0 },
			{ LMIN, LMIN }, { LMAX, LMAX }, { -2, -1, -1 } };

	@Test
	public void isSortedAndDistinctLong() {
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(LongBuffer.allocate(0), 0, 0));
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(LongBuffer.allocate(1), 0, 0));
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(LongBuffer.allocate(1), 0, 1));

		for (long[] array : SORTED_DISTINCT_LONG)
			Assertions.assertTrue(BufferUtils.isSortedAndDistinct(LongBuffer.wrap(array), 0, array.length));

		for (long[] array : NOT_SORTED_DISTINCT_LONG)
			Assertions.assertFalse(BufferUtils.isSortedAndDistinct(LongBuffer.wrap(array), 0, array.length));
	}

	// ======================================================================================

	private static final short SMIN = Short.MIN_VALUE;
	private static final short SMAX = Short.MAX_VALUE;

	private static final short[][] SORTED_SHORT = { {}, { 0 }, { 0, 0 }, { SMIN, SMIN }, { SMAX, SMAX }, { SMIN, SMAX },
			{ SMIN, 0, SMAX }, { -2, -1, 1, 2 }, };
	private static final short[][] NOT_SORTED_SHORT = { { 1, 0 }, { SMAX, SMIN }, { 3, 2, 1 }, };

	@Test
	public void isSortedShort() {
		Assertions.assertTrue(BufferUtils.isSorted(ShortBuffer.allocate(0), 0, 0));
		Assertions.assertTrue(BufferUtils.isSorted(ShortBuffer.allocate(1), 0, 0));
		Assertions.assertTrue(BufferUtils.isSorted(ShortBuffer.allocate(1), 0, 1));

		for (short[] array : SORTED_SHORT) {
			ShortBuffer b = ShortBuffer.wrap(array);
			Assertions.assertTrue(BufferUtils.isSorted(b, 0, array.length));
			if (array.length > 0) {
				Assertions.assertTrue(BufferUtils.isSorted(b, 0, array.length - 1));
				Assertions.assertTrue(BufferUtils.isSorted(b, 1, array.length));
			}
		}

		for (short[] array : NOT_SORTED_SHORT)
			Assertions.assertFalse(BufferUtils.isSorted(ShortBuffer.wrap(array), 0, array.length));
	}

	private static final short[][] SORTED_DISTINCT_SHORT = { {}, { 0 }, { SMIN, SMAX }, { SMIN, 0, SMAX },
			{ -2, -1, 1, 2 }, };
	private static final short[][] NOT_SORTED_DISTINCT_SHORT = { { 1, 0 }, { SMAX, SMIN }, { 3, 2, 1 }, { 0, 0 },
			{ SMIN, SMIN }, { SMAX, SMAX }, { -2, -1, -1 } };

	@Test
	public void isSortedAndDistinctShort() {
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(ShortBuffer.allocate(0), 0, 0));
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(ShortBuffer.allocate(1), 0, 0));
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(ShortBuffer.allocate(1), 0, 1));

		for (short[] array : SORTED_DISTINCT_SHORT)
			Assertions.assertTrue(BufferUtils.isSortedAndDistinct(ShortBuffer.wrap(array), 0, array.length));

		for (short[] array : NOT_SORTED_DISTINCT_SHORT)
			Assertions.assertFalse(BufferUtils.isSortedAndDistinct(ShortBuffer.wrap(array), 0, array.length));
	}

	// ======================================================================================

	private static final byte BMIN = Byte.MIN_VALUE;
	private static final byte BMAX = Byte.MAX_VALUE;

	private static final byte[][] SORTED_BYTE = { {}, { 0 }, { 0, 0 }, { BMIN, BMIN }, { BMAX, BMAX }, { BMIN, BMAX },
			{ BMIN, 0, BMAX }, { -2, -1, 1, 2 }, };
	private static final byte[][] NOT_SORTED_BYTE = { { 1, 0 }, { BMAX, BMIN }, { 3, 2, 1 }, };

	@Test
	public void isSortedByte() {
		Assertions.assertTrue(BufferUtils.isSorted(ByteBuffer.allocate(0), 0, 0));
		Assertions.assertTrue(BufferUtils.isSorted(ByteBuffer.allocate(1), 0, 0));
		Assertions.assertTrue(BufferUtils.isSorted(ByteBuffer.allocate(1), 0, 1));

		for (byte[] array : SORTED_BYTE) {
			ByteBuffer b = ByteBuffer.wrap(array);
			Assertions.assertTrue(BufferUtils.isSorted(b, 0, array.length));
			if (array.length > 0) {
				Assertions.assertTrue(BufferUtils.isSorted(b, 0, array.length - 1));
				Assertions.assertTrue(BufferUtils.isSorted(b, 1, array.length));
			}
		}

		for (byte[] array : NOT_SORTED_BYTE)
			Assertions.assertFalse(BufferUtils.isSorted(ByteBuffer.wrap(array), 0, array.length));
	}

	private static final byte[][] SORTED_DISTINCT_BYTE = { {}, { 0 }, { BMIN, BMAX }, { BMIN, 0, BMAX },
			{ -2, -1, 1, 2 }, };
	private static final byte[][] NOT_SORTED_DISTINCT_BYTE = { { 1, 0 }, { BMAX, BMIN }, { 3, 2, 1 }, { 0, 0 },
			{ BMIN, BMIN }, { BMAX, BMAX }, { -2, -1, -1 } };

	@Test
	public void isSortedAndDistinctByte() {
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(ByteBuffer.allocate(0), 0, 0));
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(ByteBuffer.allocate(1), 0, 0));
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(ByteBuffer.allocate(1), 0, 1));

		for (byte[] array : SORTED_DISTINCT_BYTE)
			Assertions.assertTrue(BufferUtils.isSortedAndDistinct(ByteBuffer.wrap(array), 0, array.length));

		for (byte[] array : NOT_SORTED_DISTINCT_BYTE)
			Assertions.assertFalse(BufferUtils.isSortedAndDistinct(ByteBuffer.wrap(array), 0, array.length));
	}

	// ======================================================================================

	private static final float FMIN = Float.NEGATIVE_INFINITY;
	private static final float FMAX = Float.POSITIVE_INFINITY;
	private static final float FNAN = Float.NaN;

	private static final float[][] SORTED_FLOAT = { {}, { 0 }, { 0, 0 }, { FMIN, FMIN }, { FMAX, FMAX }, { FMIN, FMAX },
			{ FMIN, 0, FMAX }, { -2, -1, 1, 2 }, { FNAN }, { 0, FNAN }, { 0, 0, FNAN }, { FMIN, FMIN, FNAN },
			{ FMAX, FMAX, FNAN }, { FMIN, FMAX, FNAN }, { FMIN, 0, FMAX, FNAN }, { -2, -1, 1, 2, FNAN } };
	private static final float[][] NOT_SORTED_FLOAT = { { 1, 0 }, { FMAX, FMIN }, { 3, 2, 1 }, { FNAN, FMAX } };

	@Test
	public void isSortedFloat() {
		Assertions.assertTrue(BufferUtils.isSorted(FloatBuffer.allocate(0), 0, 0));
		Assertions.assertTrue(BufferUtils.isSorted(FloatBuffer.allocate(1), 0, 0));
		Assertions.assertTrue(BufferUtils.isSorted(FloatBuffer.allocate(1), 0, 1));

		for (float[] array : SORTED_FLOAT) {
			FloatBuffer b = FloatBuffer.wrap(array);
			Assertions.assertTrue(BufferUtils.isSorted(b, 0, array.length));
			if (array.length > 0) {
				Assertions.assertTrue(BufferUtils.isSorted(b, 0, array.length - 1));
				Assertions.assertTrue(BufferUtils.isSorted(b, 1, array.length));
			}
		}

		for (float[] array : NOT_SORTED_FLOAT)
			Assertions.assertFalse(BufferUtils.isSorted(FloatBuffer.wrap(array), 0, array.length));
	}

	private static final float[][] SORTED_DISTINCT_FLOAT = { {}, { 0 }, { IMIN, IMAX }, { IMIN, 0, IMAX },
			{ -2, -1, 1, 2 }, };
	private static final float[][] NOT_SORTED_DISTINCT_FLOAT = { { 1, 0 }, { IMAX, IMIN }, { 3, 2, 1 }, { 0, 0 },
			{ IMIN, IMIN }, { IMAX, IMAX }, { -2, -1, -1 } };

	@Test
	public void isSortedAndDistinctFloat() {
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(FloatBuffer.allocate(0), 0, 0));
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(FloatBuffer.allocate(1), 0, 0));
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(FloatBuffer.allocate(1), 0, 1));

		for (float[] array : SORTED_DISTINCT_FLOAT)
			Assertions.assertTrue(BufferUtils.isSortedAndDistinct(FloatBuffer.wrap(array), 0, array.length));

		for (float[] array : NOT_SORTED_DISTINCT_FLOAT)
			Assertions.assertFalse(BufferUtils.isSortedAndDistinct(FloatBuffer.wrap(array), 0, array.length));
	}

	// ======================================================================================

	private static final double DMIN = Double.NEGATIVE_INFINITY;
	private static final double DMAX = Double.POSITIVE_INFINITY;
	private static final double DNAN = Double.NaN;

	private static final double[][] SORTED_DOUBLE = { {}, { 0 }, { 0, 0 }, { DMIN, DMIN }, { DMAX, DMAX },
			{ DMIN, DMAX }, { DMIN, 0, DMAX }, { -2, -1, 1, 2 }, { DNAN }, { 0, DNAN }, { 0, 0, DNAN },
			{ DMIN, DMIN, DNAN }, { DMAX, DMAX, DNAN }, { DMIN, DMAX, DNAN }, { DMIN, 0, DMAX, DNAN },
			{ -2, -1, 1, 2, DNAN } };
	private static final double[][] NOT_SORTED_DOUBLE = { { 1, 0 }, { DMAX, DMIN }, { 3, 2, 1 }, { DNAN, DMAX } };

	@Test
	public void isSortedDouble() {
		Assertions.assertTrue(BufferUtils.isSorted(DoubleBuffer.allocate(0), 0, 0));
		Assertions.assertTrue(BufferUtils.isSorted(DoubleBuffer.allocate(1), 0, 0));
		Assertions.assertTrue(BufferUtils.isSorted(DoubleBuffer.allocate(1), 0, 1));

		for (double[] array : SORTED_DOUBLE) {
			DoubleBuffer b = DoubleBuffer.wrap(array);
			Assertions.assertTrue(BufferUtils.isSorted(b, 0, array.length));
			if (array.length > 0) {
				Assertions.assertTrue(BufferUtils.isSorted(b, 0, array.length - 1));
				Assertions.assertTrue(BufferUtils.isSorted(b, 1, array.length));
			}
		}

		for (double[] array : NOT_SORTED_DOUBLE)
			Assertions.assertFalse(BufferUtils.isSorted(DoubleBuffer.wrap(array), 0, array.length));
	}

	private static final double[][] SORTED_DISTINCT_DOUBLE = { {}, { 0 }, { IMIN, IMAX }, { IMIN, 0, IMAX },
			{ -2, -1, 1, 2 }, };
	private static final double[][] NOT_SORTED_DISTINCT_DOUBLE = { { 1, 0 }, { IMAX, IMIN }, { 3, 2, 1 }, { 0, 0 },
			{ IMIN, IMIN }, { IMAX, IMAX }, { -2, -1, -1 } };

	@Test
	public void isSortedAndDistinctDouble() {
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(DoubleBuffer.allocate(0), 0, 0));
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(DoubleBuffer.allocate(1), 0, 0));
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(DoubleBuffer.allocate(1), 0, 1));

		for (double[] array : SORTED_DISTINCT_DOUBLE)
			Assertions.assertTrue(BufferUtils.isSortedAndDistinct(DoubleBuffer.wrap(array), 0, array.length));

		for (double[] array : NOT_SORTED_DISTINCT_DOUBLE)
			Assertions.assertFalse(BufferUtils.isSortedAndDistinct(DoubleBuffer.wrap(array), 0, array.length));
	}

	// ======================================================================================

	@Test
	public void testDuplicate() {
		ByteBuffer b = ByteBuffer.allocate(10);
		b.limit(8);
		b.position(5);

		ByteBuffer b2 = BufferUtils.duplicate(b.order(LITTLE_ENDIAN));
		Assertions.assertEquals(b.position(), b2.position());
		Assertions.assertEquals(b.limit(), b2.limit());
		Assertions.assertEquals(b.order(), b2.order());

		ByteBuffer b3 = BufferUtils.duplicate(b.order(BIG_ENDIAN));
		Assertions.assertEquals(b.position(), b3.position());
		Assertions.assertEquals(b.limit(), b3.limit());
		Assertions.assertEquals(b.order(), b3.order());
	}

	@Test
	public void testSlice() {
		ByteBuffer b = ByteBuffer.allocate(10);
		b.limit(8);
		b.position(5);

		ByteBuffer b2 = BufferUtils.slice(b.order(LITTLE_ENDIAN));
		Assertions.assertEquals(0, b2.position());
		Assertions.assertEquals(b.remaining(), b2.limit());
		Assertions.assertEquals(b.order(), b2.order());

		ByteBuffer b3 = BufferUtils.slice(b.order(BIG_ENDIAN));
		Assertions.assertEquals(0, b3.position());
		Assertions.assertEquals(b.remaining(), b3.limit());
		Assertions.assertEquals(b.order(), b3.order());
	}

	@Test
	public void testReadOnly() {
		ByteBuffer b = BufferUtils.allocate(1);
		Assertions.assertEquals(b.order(), ByteOrder.nativeOrder());
		b.put(0, (byte) 0x66);
		Assertions.assertEquals(b.get(0), 0x66);

		b = BufferUtils.asReadOnlyBuffer(b);
		Assertions.assertEquals(b.order(), ByteOrder.nativeOrder());
		try {
			b.put(0, (byte) 0x77);
			throw new IllegalStateException();
		} catch (ReadOnlyBufferException e) {
			// good
		}
		Assertions.assertEquals(b.get(0), 0x66);
	}

	@Test
	public void testSliceRange() {
		ByteBuffer b = ByteBuffer.wrap(new byte[] { (byte) 0, (byte) 1, (byte) 2, (byte) 3, (byte) 4 });

		ByteBuffer b2 = BufferUtils.slice(b.order(LITTLE_ENDIAN), 1, 5);
		Assertions.assertEquals(1, b2.get(0));
		Assertions.assertEquals(4, b2.get(3));
		Assertions.assertEquals(b.order(), b2.order());

		ByteBuffer b3 = BufferUtils.slice(b.order(BIG_ENDIAN), 0, 4);
		Assertions.assertEquals(0, b3.get(0));
		Assertions.assertEquals(3, b3.get(3));
		Assertions.assertEquals(b.order(), b3.order());
	}

	// ======================================================================================

	@Test
	public void testCopyByte() {
		ByteBuffer b = ByteBuffer.wrap(new byte[] { (byte) -1, (byte) 0, (byte) 1, (byte) 2, (byte) 3 });
		ByteBuffer b2 = BufferUtils.copy(b, 0, b.capacity());
		Assertions.assertArrayEquals(b.array(), b2.array());
		Assertions.assertArrayEquals(new byte[] { (byte) 0, (byte) 1, (byte) 2 }, BufferUtils.copy(b, 1, 4).array());
	}

	@Test
	public void testCopyInt() {
		int[] a = new int[] { -1, 0, 1, 2, 3 };
		IntBuffer b = IntBuffer.wrap(a);
		IntBuffer b2 = BufferUtils.copy(b, 0, b.capacity());
		int[] a2 = new int[a.length];
		b2.get(a2);
		Assertions.assertArrayEquals(a, a2);

		int[] a3 = new int[3];
		BufferUtils.copy(b, 1, 4).get(a3);
		Assertions.assertArrayEquals(new int[] { 0, 1, 2 }, a3);
	}

	@Test
	public void testCopyLong() {
		long[] a = new long[] { -1, 0, 1, 2, 3 };
		LongBuffer b = LongBuffer.wrap(a);
		LongBuffer b2 = BufferUtils.copy(b, 0, b.capacity());
		long[] a2 = new long[a.length];
		b2.get(a2);
		Assertions.assertArrayEquals(a, a2);

		long[] a3 = new long[3];
		BufferUtils.copy(b, 1, 4).get(a3);
		Assertions.assertArrayEquals(new long[] { 0, 1, 2 }, a3);
	}

	@Test
	public void testCopyShort() {
		short[] a = new short[] { -1, 0, 1, 2, 3 };
		ShortBuffer b = ShortBuffer.wrap(a);
		ShortBuffer b2 = BufferUtils.copy(b, 0, b.capacity());
		short[] a2 = new short[a.length];
		b2.get(a2);
		Assertions.assertArrayEquals(a, a2);

		short[] a3 = new short[3];
		BufferUtils.copy(b, 1, 4).get(a3);
		Assertions.assertArrayEquals(new short[] { 0, 1, 2 }, a3);
	}

	@Test
	public void testCopyFloat() {
		float[] a = new float[] { -1, 0, 1, 2, 3 };
		FloatBuffer b = FloatBuffer.wrap(a);
		FloatBuffer b2 = BufferUtils.copy(b, 0, b.capacity());
		float[] a2 = new float[a.length];
		b2.get(a2);
		Assertions.assertArrayEquals(a, a2);

		float[] a3 = new float[3];
		BufferUtils.copy(b, 1, 4).get(a3);
		Assertions.assertArrayEquals(new float[] { 0, 1, 2 }, a3);
	}

	@Test
	public void testCopyDouble() {
		double[] a = new double[] { -1, 0, 1, 2, 3 };
		DoubleBuffer b = DoubleBuffer.wrap(a);
		DoubleBuffer b2 = BufferUtils.copy(b, 0, b.capacity());
		double[] a2 = new double[a.length];
		b2.get(a2);
		Assertions.assertArrayEquals(a, a2);

		double[] a3 = new double[3];
		BufferUtils.copy(b, 1, 4).get(a3);
		Assertions.assertArrayEquals(new double[] { 0, 1, 2 }, a3);
	}

	// ======================================================================================

	@Test
	public void deduplicateInt() {

		deuplicateInt(new int[] { 0, 1, 2, 2, 3, 3, 3, 4, 4, 4, 4 }, new int[] { 0, 1, 2, 3, 4 });
		deuplicateInt(new int[] { 0, 1, 2, 3, 4 }, new int[] { 0, 1, 2, 3, 4 });
		deuplicateInt(new int[] {}, new int[] {});
		deuplicateInt(new int[] { 0 }, new int[] { 0 });
		deuplicateInt(new int[] { 0, 0 }, new int[] { 0 });
		deuplicateInt(new int[] { 0, 0, 0 }, new int[] { 0 });
		deuplicateInt(new int[] { 0, 0, 1, 1 }, new int[] { 0, 1 });
		deuplicateInt(new int[] { 0, 0, 0, 1, 1 }, new int[] { 0, 1 });
		deuplicateInt(new int[] { 0, 0, 1, 1 }, new int[] { 0, 1 });
		deuplicateInt(new int[] { 0, 0, 0, 1, 1 }, new int[] { 0, 1 });
		deuplicateInt(new int[] { 0, 1, 1 }, new int[] { 0, 1 });
		deuplicateInt(new int[] { 0, 1, 1, 2 }, new int[] { 0, 1, 2 });
	}

	private static void deuplicateInt(int[] before, int[] after) {

		int highest = BufferUtils.deduplicate(IntBuffer.wrap(before), 0, before.length);
		Assertions.assertEquals(after.length, highest);
		Assertions.assertArrayEquals(after, Arrays.copyOf(before, after.length));
	}

	@Test
	public void deduplicateLong() {

		deduplicateLong(new long[] { 0, 1, 2, 2, 3, 3, 3, 4, 4, 4, 4 }, new long[] { 0, 1, 2, 3, 4 });
		deduplicateLong(new long[] { 0, 1, 2, 3, 4 }, new long[] { 0, 1, 2, 3, 4 });
		deduplicateLong(new long[] {}, new long[] {});
		deduplicateLong(new long[] { 0 }, new long[] { 0 });
		deduplicateLong(new long[] { 0, 0 }, new long[] { 0 });
		deduplicateLong(new long[] { 0, 0, 0 }, new long[] { 0 });
		deduplicateLong(new long[] { 0, 0, 1, 1 }, new long[] { 0, 1 });
		deduplicateLong(new long[] { 0, 0, 0, 1, 1 }, new long[] { 0, 1 });
		deduplicateLong(new long[] { 0, 0, 1, 1 }, new long[] { 0, 1 });
		deduplicateLong(new long[] { 0, 0, 0, 1, 1 }, new long[] { 0, 1 });
		deduplicateLong(new long[] { 0, 1, 1 }, new long[] { 0, 1 });
		deduplicateLong(new long[] { 0, 1, 1, 2 }, new long[] { 0, 1, 2 });
	}

	private static void deduplicateLong(long[] before, long[] after) {

		long highest = BufferUtils.deduplicate(LongBuffer.wrap(before), 0, before.length);
		Assertions.assertEquals(after.length, highest);
		Assertions.assertArrayEquals(after, Arrays.copyOf(before, after.length));
	}

	@Test
	public void deduplicateShort() {

		deduplicateShort(new short[] { 0, 1, 2, 2, 3, 3, 3, 4, 4, 4, 4 }, new short[] { 0, 1, 2, 3, 4 });
		deduplicateShort(new short[] { 0, 1, 2, 3, 4 }, new short[] { 0, 1, 2, 3, 4 });
		deduplicateShort(new short[] {}, new short[] {});
		deduplicateShort(new short[] { 0 }, new short[] { 0 });
		deduplicateShort(new short[] { 0, 0 }, new short[] { 0 });
		deduplicateShort(new short[] { 0, 0, 0 }, new short[] { 0 });
		deduplicateShort(new short[] { 0, 0, 1, 1 }, new short[] { 0, 1 });
		deduplicateShort(new short[] { 0, 0, 0, 1, 1 }, new short[] { 0, 1 });
		deduplicateShort(new short[] { 0, 0, 1, 1 }, new short[] { 0, 1 });
		deduplicateShort(new short[] { 0, 0, 0, 1, 1 }, new short[] { 0, 1 });
		deduplicateShort(new short[] { 0, 1, 1 }, new short[] { 0, 1 });
		deduplicateShort(new short[] { 0, 1, 1, 2 }, new short[] { 0, 1, 2 });
	}

	private static void deduplicateShort(short[] before, short[] after) {

		int highest = BufferUtils.deduplicate(ShortBuffer.wrap(before), 0, before.length);
		Assertions.assertEquals(after.length, highest);
		Assertions.assertArrayEquals(after, Arrays.copyOf(before, after.length));
	}

	@Test
	public void deduplicateByte() {

		deduplicateByte(new byte[] { 0, 1, 2, 2, 3, 3, 3, 4, 4, 4, 4 }, new byte[] { 0, 1, 2, 3, 4 });
		deduplicateByte(new byte[] { 0, 1, 2, 3, 4 }, new byte[] { 0, 1, 2, 3, 4 });
		deduplicateByte(new byte[] {}, new byte[] {});
		deduplicateByte(new byte[] { 0 }, new byte[] { 0 });
		deduplicateByte(new byte[] { 0, 0 }, new byte[] { 0 });
		deduplicateByte(new byte[] { 0, 0, 0 }, new byte[] { 0 });
		deduplicateByte(new byte[] { 0, 0, 1, 1 }, new byte[] { 0, 1 });
		deduplicateByte(new byte[] { 0, 0, 0, 1, 1 }, new byte[] { 0, 1 });
		deduplicateByte(new byte[] { 0, 0, 1, 1 }, new byte[] { 0, 1 });
		deduplicateByte(new byte[] { 0, 0, 0, 1, 1 }, new byte[] { 0, 1 });
		deduplicateByte(new byte[] { 0, 1, 1 }, new byte[] { 0, 1 });
		deduplicateByte(new byte[] { 0, 1, 1, 2 }, new byte[] { 0, 1, 2 });
	}

	private static void deduplicateByte(byte[] before, byte[] after) {

		int highest = BufferUtils.deduplicate(ByteBuffer.wrap(before), 0, before.length);
		Assertions.assertEquals(after.length, highest);
		Assertions.assertArrayEquals(after, Arrays.copyOf(before, after.length));
	}

	@Test
	public void deduplicateFloat() {

		deduplicateFloat(new float[] { 0, 1, 2, 2, 3, 3, 3, 4, 4, 4, 4 }, new float[] { 0, 1, 2, 3, 4 });
		deduplicateFloat(new float[] { 0, 1, 2, 3, 4 }, new float[] { 0, 1, 2, 3, 4 });
		deduplicateFloat(new float[] {}, new float[] {});
		deduplicateFloat(new float[] { 0 }, new float[] { 0 });
		deduplicateFloat(new float[] { 0, 0 }, new float[] { 0 });
		deduplicateFloat(new float[] { 0, 0, 0 }, new float[] { 0 });
		deduplicateFloat(new float[] { 0, 0, 1, 1 }, new float[] { 0, 1 });
		deduplicateFloat(new float[] { 0, 0, 0, 1, 1 }, new float[] { 0, 1 });
		deduplicateFloat(new float[] { 0, 0, 1, 1 }, new float[] { 0, 1 });
		deduplicateFloat(new float[] { 0, 0, 0, 1, 1 }, new float[] { 0, 1 });
		deduplicateFloat(new float[] { 0, 1, 1 }, new float[] { 0, 1 });
		deduplicateFloat(new float[] { 0, 1, 1, 2 }, new float[] { 0, 1, 2 });
		deduplicateFloat(new float[] { Float.NaN }, new float[] { Float.NaN });
		deduplicateFloat(new float[] { Float.NaN, Float.NaN }, new float[] { Float.NaN });
		deduplicateFloat(new float[] { 0, Float.NaN, Float.NaN }, new float[] { 0, Float.NaN });
	}

	private static void deduplicateFloat(float[] before, float[] after) {

		float highest = BufferUtils.deduplicate(FloatBuffer.wrap(before), 0, before.length);
		Assertions.assertEquals(after.length, highest);
		Assertions.assertArrayEquals(after, Arrays.copyOf(before, after.length));
	}

	@Test
	public void deduplicateDouble() {

		deduplicateDouble(new double[] { 0, 1, 2, 2, 3, 3, 3, 4, 4, 4, 4 }, new double[] { 0, 1, 2, 3, 4 });
		deduplicateDouble(new double[] { 0, 1, 2, 3, 4 }, new double[] { 0, 1, 2, 3, 4 });
		deduplicateDouble(new double[] {}, new double[] {});
		deduplicateDouble(new double[] { 0 }, new double[] { 0 });
		deduplicateDouble(new double[] { 0, 0 }, new double[] { 0 });
		deduplicateDouble(new double[] { 0, 0, 0 }, new double[] { 0 });
		deduplicateDouble(new double[] { 0, 0, 1, 1 }, new double[] { 0, 1 });
		deduplicateDouble(new double[] { 0, 0, 0, 1, 1 }, new double[] { 0, 1 });
		deduplicateDouble(new double[] { 0, 0, 1, 1 }, new double[] { 0, 1 });
		deduplicateDouble(new double[] { 0, 0, 0, 1, 1 }, new double[] { 0, 1 });
		deduplicateDouble(new double[] { 0, 1, 1 }, new double[] { 0, 1 });
		deduplicateDouble(new double[] { 0, 1, 1, 2 }, new double[] { 0, 1, 2 });
		deduplicateDouble(new double[] { Double.NaN }, new double[] { Double.NaN });
		deduplicateDouble(new double[] { Double.NaN, Double.NaN }, new double[] { Double.NaN });
		deduplicateDouble(new double[] { 0, Double.NaN, Double.NaN }, new double[] { 0, Double.NaN });
	}

	private static void deduplicateDouble(double[] before, double[] after) {

		double highest = BufferUtils.deduplicate(DoubleBuffer.wrap(before), 0, before.length);
		Assertions.assertEquals(after.length, highest);
		Assertions.assertArrayEquals(after, Arrays.copyOf(before, after.length));
	}

	// ======================================================================================

	@Test
	public void intStream() {
		int[] expected = { IMAX, 1, 2, 3, 5, 4, IMIN };
		int[] actual = BufferUtils.stream(IntBuffer.wrap(expected)).toArray();
		Assertions.assertArrayEquals(expected, actual);
	}

	@Test
	public void intStream2() {
		int[] array = new int[100000];
		Arrays.fill(array, 1);
		IntBuffer buf = IntBuffer.wrap(array);

		int expected = 0;
		for (Iterator<Integer> iter = BufferUtils.stream(buf).iterator(); iter.hasNext();)
			expected += iter.next();

		int actual = BufferUtils.stream(buf).parallel().sum();

		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void longStream() {
		long[] expected = { LMAX, 1, 2, 3, 5, 4, LMIN };
		long[] actual = BufferUtils.stream(LongBuffer.wrap(expected)).toArray();
		Assertions.assertArrayEquals(expected, actual);
	}

	@Test
	public void longStream2() {
		long[] array = new long[100000];
		Arrays.fill(array, 1);
		LongBuffer buf = LongBuffer.wrap(array);

		long expected = 0;
		for (Iterator<Long> iter = BufferUtils.stream(buf).iterator(); iter.hasNext();)
			expected += iter.next();

		long actual = BufferUtils.stream(buf).parallel().sum();

		Assertions.assertEquals(expected, actual);
	}

	@Test
	public void doubleStream() {
		double[] expected = { DMAX, 1, 2, 3, DNAN, 4, DMIN };
		double[] actual = BufferUtils.stream(DoubleBuffer.wrap(expected)).toArray();
		Assertions.assertArrayEquals(expected, actual);
	}

	@Test
	public void doubleStream2() {
		double[] array = new double[100000];
		Arrays.fill(array, 1);
		DoubleBuffer buf = DoubleBuffer.wrap(array);

		double expected = 0;
		for (Iterator<Double> iter = BufferUtils.stream(buf).iterator(); iter.hasNext();)
			expected += iter.next();

		double actual = BufferUtils.stream(buf).parallel().sum();

		Assertions.assertEquals(expected, actual);
	}

	// ======================================================================================

	@Test
	public void rangeCheck() {
		try {
			BufferUtils.rangeCheck(10, 5, 3);
			throw new RuntimeException("Expected IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			// good
		}
		try {
			BufferUtils.rangeCheck(10, -1, 3);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException e) {
			// good
		}
		try {
			BufferUtils.rangeCheck(10, 5, 11);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException e) {
			// good
		}
	}
}
