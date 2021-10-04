package tech.bitey.bufferstuff;

import static java.lang.Integer.bitCount;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static tech.bitey.bufferstuff.BufferUtils.allocate;
import static tech.bitey.bufferstuff.BufferUtils.duplicate;
import static tech.bitey.bufferstuff.BufferUtils.readFully;
import static tech.bitey.bufferstuff.BufferUtils.slice;
import static tech.bitey.bufferstuff.BufferUtils.writeFully;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.BitSet;
import java.util.Random;

/**
 * Similar to {@link java.util.BitSet BitSet}, but backed by a
 * {@link java.nio.ByteBuffer ByteBuffer}. Differences with {@code BitSet}
 * include:
 * <p>
 * {@code BufferBitSet}
 * <ul>
 * <li>... is not {@code Serializable}.
 * <li>... does not hide the backing buffer, and offers copy-free methods for
 * wrapping an existing buffer.
 * <li>... allows for specifying whether or not the buffer can be resized
 * (replaced with a larger buffer)
 * </ul>
 * This bitset implementation is not thread safe, and concurrent writes or
 * external modifications to the backing buffer could put it into a bad state.
 * <p>
 * The {@link #isResizable() resizable} flag controls whether or not the bitset
 * can grow to accommodate setting bits beyond the current buffer's limit (by
 * replacing the current buffer with a larger one).
 * <p>
 * All {@code ByteBuffers} allocated by this class are procured via
 * {@link BufferUtils#allocate(int)}. The allocated buffers will be direct if
 * the {@code tech.bitey.allocateDirect} system property is set to "true".
 * 
 * @author biteytech@protonmail.com, adapted from java.util.BitSet
 * 
 * @see java.util.BitSet
 * @see java.nio.ByteBuffer
 */
public class BufferBitSet implements Cloneable {

	/** An empty, non-resizable {@link BufferBitSet} */
	public static final BufferBitSet EMPTY_BITSET = new BufferBitSet(false);

	private static final int DEFAULT_INITIAL_SIZE = 8;

	private static final int MASK = 0xFF;

	private static final int MAX_CAPACITY = byteIndex(Integer.MAX_VALUE) + 1;

	/**
	 * Specifies whether or not the buffer can be replaced with a larger one.
	 */
	private final boolean resizable;

	/**
	 * This buffer's {@link ByteBuffer#limit() limit} is always equal to its
	 * {@link ByteBuffer#capacity() capacity}. The {@link ByteBuffer#position()
	 * position} is used to track how many bytes are actually in use.
	 */
	private ByteBuffer buffer;

	/*--------------------------------------------------------------------------------
	 *  Getters
	 *-------------------------------------------------------------------------------*/
	/**
	 * Returns the {@link ByteBuffer} backing this {@link BufferBitSet}.
	 * 
	 * @return the {@link ByteBuffer} backing this {@link BufferBitSet}.
	 */
	public ByteBuffer getBuffer() {
		return buffer;
	}

	/**
	 * Returns true if this bitset's buffer can be resized (replaced with a larger
	 * buffer).
	 * 
	 * @return true if this bitset's buffer can be resized.
	 */
	public boolean isResizable() {
		return resizable;
	}

	/*--------------------------------------------------------------------------------
	 *  Constructors and factory methods
	 *-------------------------------------------------------------------------------*/
	/**
	 * "Master" constructor. All other constructors invoke this one.
	 */
	private BufferBitSet(ByteBuffer buffer, boolean resizable, boolean externalBuffer) {

		if (buffer == null)
			throw new NullPointerException("buffer cannot be null");

		if (externalBuffer) {
			this.buffer = buffer.slice();
			this.buffer.limit(Math.min(this.buffer.limit(), MAX_CAPACITY));
			this.buffer.position(this.buffer.limit());
			recalculateBytesInUse();
		} else {
			this.buffer = buffer;
		}

		this.resizable = resizable;
	}

	/**
	 * Creates an empty, resizable {@link BufferBitSet}
	 */
	public BufferBitSet() {
		this(true);
	}

	/**
	 * Creates an empty {@link BufferBitSet} with the specified resize behavior.
	 * 
	 * @param resizable - specifies whether or not the buffer can be resized
	 *                  (replaced with a larger buffer)
	 */
	public BufferBitSet(boolean resizable) {
		this(allocate(DEFAULT_INITIAL_SIZE), resizable, false);
	}

	/**
	 * Creates a {@link BufferBitSet} which wraps the provided buffer. This bitset
	 * will only make use of the space demarked by {@link ByteBuffer#position()} and
	 * {@link ByteBuffer#limit()}. The provided buffer object will not itself be
	 * modified, though the buffer's content can be via writes to this bitset.
	 * <p>
	 * The resulting bitset is not resizable.
	 * 
	 * @param buffer - the {@link ByteBuffer} to be wrapped by this bitset. Writes
	 *               to this bitset will modify the buffer's content.
	 * 
	 * @throws NullPointerException if the provided buffer is null
	 */
	public BufferBitSet(ByteBuffer buffer) {
		this(buffer, false, true);
	}

	/**
	 * Creates a {@link BufferBitSet} which wraps the provided buffer. This bitset
	 * will only make use of the space demarked by {@link ByteBuffer#position()} and
	 * {@link ByteBuffer#limit()}. The provided buffer object will not itself be
	 * modified, though the buffer's content can be via writes to this bitset.
	 * 
	 * @param buffer    - the {@link ByteBuffer} to be wrapped by this bitset.
	 *                  Writes to this bitset will modify the buffer's content.
	 * @param resizable - specifies whether or not the buffer can be resized
	 *                  (replaced with a larger buffer)
	 */
	public BufferBitSet(ByteBuffer buffer, boolean resizable) {
		this(buffer, resizable, true);
	}

	/**
	 * Returns a new resizable bitset containing all of the bits in the given byte
	 * array.
	 * <p>
	 * More precisely, <br>
	 * {@code BufferBitSet.valueOf(bytes).get(n) == ((bytes[n/8] & (1<<(n%8))) != 0)}
	 * <br>
	 * for all {@code n <  8 * bytes.length}.
	 * <p>
	 * <em>The provided array is wrapped, it is not copied.</em> Writes to this
	 * bitset can modify the array.
	 *
	 * @param bytes - a byte array containing a sequence of bits to be used as the
	 *              initial bits of the new bit set
	 * 
	 * @return a new resizable bitset containing all of the bits in the given byte
	 *         array.
	 */
	public static BufferBitSet valueOf(byte[] bytes) {

		ByteBuffer buffer = ByteBuffer.wrap(bytes);

		// find last set bit
		int n = buffer.limit() - 1;
		while (n >= 0 && buffer.get(n) == 0)
			n--;

		buffer.position(n + 1);

		return new BufferBitSet(buffer, true, false);
	}

	/**
	 * Returns a new resizable {@link BufferBitSet} containing all of the bits in
	 * the given {@link java.util.BitSet}.
	 *
	 * @param bs - the bitset to copy
	 * 
	 * @return a new resizable {@code BufferBitSet} containing all of the bits in
	 *         the given {@code java.util.BitSet}.
	 */
	public static BufferBitSet valueOf(BitSet bs) {

		byte[] array = bs.toByteArray();
		ByteBuffer buffer = ByteBuffer.wrap(array);

		buffer.limit(array.length);
		buffer.position(array.length);

		return new BufferBitSet(buffer, true, false);
	}

	/**
	 * Returns a new {@link BufferBitSet} with the specified resizability. The
	 * buffer object itself will be {@link ByteBuffer#duplicate duplicated}, but
	 * will share the underlying space.
	 * 
	 * @param resizable - specifies whether or not the buffer can be resized
	 *                  (replaced with a larger buffer)
	 * 
	 * @return a new bitset with the specified resize behavior
	 */
	public BufferBitSet resizable(boolean resizable) {
		return new BufferBitSet(duplicate(buffer), resizable, false);
	}

	/**
	 * Returns a new {@link BufferBitSet} with {@code n} bits set randomly in the
	 * range zero to {@code size} (exclusive).
	 * 
	 * @param n    - the number of bits to set
	 * @param size - bits are set within the range zero to size (exclusive)
	 * 
	 * @return a new bitset with n bits set randomly in the range zero to size
	 *         (exclusive)
	 * 
	 * @throws IllegalArgumentException if {@code size < 0}
	 * @throws IllegalArgumentException if {@code n < 0 || n > size}
	 */
	public static BufferBitSet random(int n, int size) {
		final Random random = new Random();
		return random(n, size, random);
	}

	static BufferBitSet random(int n, int size, Random random) {
		if (size < 0)
			throw new IllegalArgumentException("size must be > 1");
		if (n < 0 || n > size)
			throw new IllegalArgumentException("n must between 0 and size inclusive");

		BufferBitSet bbs = new BufferBitSet();
		bbs.set(0, n);

		for (int i = size; i > 1; i--) {
			int r = random.nextInt(i);
			boolean temp = bbs.get(r);
			bbs.set(r, bbs.get(i - 1));
			bbs.set(i - 1, temp);
		}

		return bbs;
	}

	/*--------------------------------------------------------------------------------
	 *  Methods which export the bits to different formats
	 *-------------------------------------------------------------------------------*/
	/**
	 * Returns a new byte array containing all the bits in this bit set.
	 * 
	 * @return a new byte array containing all the bits in this bit set.
	 */
	public byte[] toByteArray() {

		ByteBuffer buffer = this.buffer.duplicate();
		buffer.flip();

		byte[] array = new byte[buffer.limit()];
		buffer.get(array);

		return array;
	}

	/**
	 * Returns a new {@link java.util.BitSet} containing all of the bits in this
	 * {@link BufferBitSet}.
	 * 
	 * @return a new {@link java.util.BitSet} containing all of the bits in this
	 *         {@link BufferBitSet}.
	 */
	public BitSet toBitSet() {
		return BitSet.valueOf(toByteArray());
	}

	/*--------------------------------------------------------------------------------
	 *  Methods for reading from and writing to a channel
	 *-------------------------------------------------------------------------------*/

	/**
	 * Write this bitset to the specified {@link WritableByteChannel}. Equivalent to
	 * {@link #writeTo(WritableByteChannel, int, int) writeTo(channel, 0, length())}
	 * 
	 * @param channel - the channel to write to
	 * 
	 * @throws IOException if some I/O error occurs
	 */
	public void writeTo(WritableByteChannel channel) throws IOException {
		writeTo(channel, 0, lastSetBit() + 1);
	}

	/**
	 * Write a range from this bitset to the specified {@link WritableByteChannel}.
	 * This method will write a 5-byte header followed by the bytes which store the
	 * bits in the specified range.
	 * 
	 * @param channel   - the channel to write to
	 * @param fromIndex - index of the first bit to write
	 * @param toIndex   - index after the last bit to write
	 * 
	 * @throws IOException               if some I/O error occurs
	 * @throws IndexOutOfBoundsException if {@code fromIndex} is negative, or
	 *                                   {@code toIndex} is negative, or
	 *                                   {@code fromIndex} is larger than
	 *                                   {@code toIndex}
	 */
	public void writeTo(WritableByteChannel channel, int fromIndex, int toIndex) throws IOException {
		checkRange(fromIndex, toIndex);

		final int lastSetBit = lastSetBit();

		if (fromIndex > lastSetBit) {
			fromIndex = 0;
			toIndex = 0;
		} else if (toIndex - 1 > lastSetBit)
			toIndex = lastSetBit + 1;

		ByteBuffer buffer = slice(this.buffer, byteIndex(fromIndex), byteIndex(toIndex - 1) + 1);

		// find last set bit
		int n = buffer.limit() - 1;
		while (n >= 0 && buffer.get(n) == 0)
			n--;
		buffer.limit(n + 1);
		final int limit = buffer.limit();

		ByteBuffer header = ByteBuffer.allocate(5).order(BIG_ENDIAN);
		header.put(0, (byte) (fromIndex & 7));
		header.putInt(1, limit);

		writeFully(channel, header);

		if (limit > 0) {
			if (limit > 1) {
				// write everything except last byte
				writeFully(channel, slice(buffer, 0, limit - 1));
			}

			// handle last byte
			ByteBuffer lastByte = ByteBuffer.allocate(1);
			lastByte.put(0, (byte) (buffer.get(limit - 1) & (MASK >>> ((-toIndex) & 7))));
			writeFully(channel, lastByte);
		}
	}

	/**
	 * Read a bitset from the specified {@link ReadableByteChannel}. The bitset must
	 * have been previously written with one of the {@code writeTo} methods.
	 * 
	 * @param channel - the channel to read from
	 * 
	 * @return a non-resizable bitset from the specified channel
	 * 
	 * @throws IOException if some I/O error occurs
	 */
	public static BufferBitSet readFrom(ReadableByteChannel channel) throws IOException {

		ByteBuffer header = ByteBuffer.allocate(5).order(BIG_ENDIAN);
		readFully(channel, header);

		int offset = header.get(0);
		int capacity = header.getInt(1);

		if (capacity == 0)
			return EMPTY_BITSET;

		ByteBuffer buffer = allocate(capacity);
		readFully(channel, buffer);

		if (offset == 0)
			return new BufferBitSet(buffer, false, false);

		// left shift by offset
		int limit = buffer.limit();
		for (int i = 0; i < limit - 1; i++)
			buffer.put(i, (byte) (((buffer.get(i) & 0xFF) >>> offset) | (buffer.get(i + 1) << ((-offset) & 7))));

		// handle last byte
		buffer.put(limit - 1, (byte) ((buffer.get(limit - 1) & 0xFF) >>> offset));

		buffer.clear();
		return new BufferBitSet(buffer, false, true);
	}

	/*--------------------------------------------------------------------------------
	 *  Get / Set / Flip / Clear
	 *-------------------------------------------------------------------------------*/
	/**
	 * Returns the value of the bit with the specified index. The value is
	 * {@code true} if the bit with the index {@code bitIndex} is currently set in
	 * this bitset; otherwise, the result is {@code false}.
	 *
	 * @param bitIndex the bit index
	 * @return the value of the bit with the specified index
	 * @throws IndexOutOfBoundsException if the specified index is negative
	 */
	public boolean get(int bitIndex) {
		if (bitIndex < 0)
			throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

		int byteIndex = byteIndex(bitIndex);
		return (byteIndex < buffer.position()) && ((byt(byteIndex) & bit(bitIndex)) != 0);
	}

	/**
	 * Returns a new {@code BufferBitSet} composed of bits from this bitset from
	 * {@code fromIndex} (inclusive) to {@code toIndex} (exclusive).
	 * <p>
	 * The resulting bitset will always be stored in newly allocated space, and will
	 * have the same resizable settings as this bitset.
	 *
	 * @param fromIndex - index of the first bit to include
	 * @param toIndex   - index after the last bit to include
	 * @return a new bitset from a range of this bitset
	 * @throws IndexOutOfBoundsException if {@code fromIndex} is negative, or
	 *                                   {@code toIndex} is negative, or
	 *                                   {@code fromIndex} is larger than
	 *                                   {@code toIndex}
	 */
	public BufferBitSet get(int fromIndex, int toIndex) {

		final LeftShift shift = new LeftShift(fromIndex, toIndex);

		// If no bits set in range then return empty bitset
		if (shift.byteCount == 0)
			return new BufferBitSet(resizable);

		ByteBuffer resultBuffer = allocate(shift.byteCount);
		BufferBitSet result = new BufferBitSet(resultBuffer, resizable, false);

		for (int i = 0; i < shift.byteCount; i++)
			resultBuffer.put((byte) shift.get(i));

		result.recalculateBytesInUse();

		return result;
	}

	private class LeftShift {

		final int fromIndex;
		final int toIndex;
		final int byteCount;

		final int sourceIndex;
		final boolean byteAligned;

		LeftShift(int fromIndex, int toIndex) {
			checkRange(fromIndex, toIndex);

			final int lastSetBit = lastSetBit();

			if (lastSetBit < fromIndex || fromIndex == toIndex) {
				// no bits set in range
				this.fromIndex = this.toIndex = byteCount = sourceIndex = 0;
				this.byteAligned = false;
				return;
			}

			// An optimization
			if (toIndex - 1 > lastSetBit)
				toIndex = lastSetBit + 1;

			byteCount = byteIndex(toIndex - fromIndex - 1) + 1;

			this.fromIndex = fromIndex;
			this.toIndex = toIndex;

			this.sourceIndex = byteIndex(fromIndex);
			this.byteAligned = ((fromIndex & 7) == 0);
		}

		int get(int byteIndex) {

			final int index = sourceIndex + byteIndex;

			if (byteIndex < byteCount - 1) {
				// all bytes but the last one
				return byteAligned ? byt(index)
						: ((byt(index) & 0xFF) >>> (fromIndex & 7)) | (byt(index + 1) << ((-fromIndex) & 7));
			} else {
				// last byte
				int lastWordMask = MASK >>> ((-toIndex) & 7);
				return ((toIndex - 1) & 7) < (fromIndex & 7) ? /* straddles source bytes */
						(((byt(index) & 0xFF) >>> (fromIndex & 7))
								| (byt(index + 1) & lastWordMask) << ((-fromIndex) & 7))
						: ((byt(index) & lastWordMask) >>> (fromIndex & 7));
			}
		}
	}

	/**
	 * Sets the bit at the specified index to {@code true}.
	 *
	 * @param bitIndex a bit index
	 * @throws IndexOutOfBoundsException if the specified index is negative
	 */
	public void set(int bitIndex) {
		if (bitIndex < 0)
			throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

		int byteIndex = byteIndex(bitIndex);
		expandTo(byteIndex);

		put(byteIndex, byt(byteIndex) | bit(bitIndex));
	}

	/**
	 * Sets the bit at the specified index to the specified value.
	 *
	 * @param bitIndex a bit index
	 * @param value    a boolean value to set
	 * @throws IndexOutOfBoundsException if the specified index is negative
	 */
	public void set(int bitIndex, boolean value) {
		if (value)
			set(bitIndex);
		else
			clear(bitIndex);
	}

	/**
	 * Sets the bits from the specified {@code fromIndex} (inclusive) to the
	 * specified {@code toIndex} (exclusive) to {@code true}.
	 *
	 * @param fromIndex index of the first bit to be set
	 * @param toIndex   index after the last bit to be set
	 * @throws IndexOutOfBoundsException if {@code fromIndex} is negative, or
	 *                                   {@code toIndex} is negative, or
	 *                                   {@code fromIndex} is larger than
	 *                                   {@code toIndex}
	 */
	public void set(int fromIndex, int toIndex) {
		checkRange(fromIndex, toIndex);

		if (fromIndex == toIndex)
			return;

		// Increase capacity if necessary
		int startByteIndex = byteIndex(fromIndex);
		int endByteIndex = byteIndex(toIndex - 1);
		expandTo(endByteIndex);

		int firstByteMask = MASK << (fromIndex & 7);
		int lastByteMask = MASK >>> ((-toIndex) & 7);

		if (startByteIndex == endByteIndex) {
			// Case 1: One word
			put(startByteIndex, byt(startByteIndex) | (firstByteMask & lastByteMask));
		} else {
			// Case 2: Multiple words
			// Handle first word
			put(startByteIndex, byt(startByteIndex) | firstByteMask);

			// Handle intermediate words, if any
			for (int i = startByteIndex + 1; i < endByteIndex; i++)
				put(i, MASK);

			// Handle last word
			put(endByteIndex, byt(endByteIndex) | lastByteMask);
		}
	}

	/**
	 * Sets the bits from the specified {@code fromIndex} (inclusive) to the
	 * specified {@code toIndex} (exclusive) to the specified value.
	 *
	 * @param fromIndex index of the first bit to be set
	 * @param toIndex   index after the last bit to be set
	 * @param value     value to set the selected bits to
	 * @throws IndexOutOfBoundsException if {@code fromIndex} is negative, or
	 *                                   {@code toIndex} is negative, or
	 *                                   {@code fromIndex} is larger than
	 *                                   {@code toIndex}
	 */
	public void set(int fromIndex, int toIndex, boolean value) {
		if (value)
			set(fromIndex, toIndex);
		else
			clear(fromIndex, toIndex);
	}

	/**
	 * Sets the bit at the specified index to the complement of its current value.
	 *
	 * @param bitIndex the index of the bit to flip
	 * @throws IndexOutOfBoundsException if the specified index is negative
	 */
	public void flip(int bitIndex) {
		if (bitIndex < 0)
			throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

		int byteIndex = byteIndex(bitIndex);
		expandTo(byteIndex);

		put(byteIndex, byt(byteIndex) ^ bit(bitIndex));

		recalculateBytesInUse();
	}

	/**
	 * Sets each bit from the specified {@code fromIndex} (inclusive) to the
	 * specified {@code toIndex} (exclusive) to the complement of its current value.
	 *
	 * @param fromIndex index of the first bit to flip
	 * @param toIndex   index after the last bit to flip
	 * @throws IndexOutOfBoundsException if {@code fromIndex} is negative, or
	 *                                   {@code toIndex} is negative, or
	 *                                   {@code fromIndex} is larger than
	 *                                   {@code toIndex}
	 */
	public void flip(int fromIndex, int toIndex) {
		checkRange(fromIndex, toIndex);

		if (fromIndex == toIndex)
			return;

		// Increase capacity if necessary
		int startByteIndex = byteIndex(fromIndex);
		int endByteIndex = byteIndex(toIndex - 1);
		expandTo(endByteIndex);

		int firstByteMask = MASK << (fromIndex & 7);
		int lastByteMask = MASK >>> ((-toIndex) & 7);

		if (startByteIndex == endByteIndex) {
			// Case 1: One word
			put(startByteIndex, byt(startByteIndex) ^ (firstByteMask & lastByteMask));
		} else {
			// Case 2: Multiple words
			// Handle first word
			put(startByteIndex, byt(startByteIndex) ^ firstByteMask);

			// Handle intermediate words, if any
			for (int i = startByteIndex + 1; i < endByteIndex; i++)
				put(i, byt(i) ^ MASK);

			// Handle last word
			put(endByteIndex, byt(endByteIndex) ^ lastByteMask);
		}

		recalculateBytesInUse();
	}

	/**
	 * Sets the bit specified by the index to {@code false}.
	 *
	 * @param bitIndex the index of the bit to be cleared
	 * @throws IndexOutOfBoundsException if the specified index is negative
	 */
	public void clear(int bitIndex) {
		if (bitIndex < 0)
			throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

		int byteIndex = byteIndex(bitIndex);
		if (byteIndex >= buffer.position())
			return;

		put(byteIndex, byt(byteIndex) & ~bit(bitIndex));

		recalculateBytesInUse();
	}

	/**
	 * Sets the bits from the specified {@code fromIndex} (inclusive) to the
	 * specified {@code toIndex} (exclusive) to {@code false}.
	 *
	 * @param fromIndex index of the first bit to be cleared
	 * @param toIndex   index after the last bit to be cleared
	 * @throws IndexOutOfBoundsException if {@code fromIndex} is negative, or
	 *                                   {@code toIndex} is negative, or
	 *                                   {@code fromIndex} is larger than
	 *                                   {@code toIndex}
	 */
	public void clear(int fromIndex, int toIndex) {
		checkRange(fromIndex, toIndex);

		final int lastSetBit = lastSetBit();

		// If no set bits in range return
		if (lastSetBit < fromIndex || fromIndex == toIndex)
			return;

		// An optimization
		if (toIndex - 1 > lastSetBit)
			toIndex = lastSetBit + 1;

		int startByteIndex = byteIndex(fromIndex);
		int endByteIndex = byteIndex(toIndex - 1);

		int firstByteMask = MASK << (fromIndex & 7);
		int lastByteMask = MASK >>> ((-toIndex) & 7);

		if (startByteIndex == endByteIndex) {
			// Case 1: One word
			put(startByteIndex, byt(startByteIndex) & ~(firstByteMask & lastByteMask));
		} else {
			// Case 2: Multiple words
			// Handle first word
			put(startByteIndex, byt(startByteIndex) & ~firstByteMask);

			// Handle intermediate words, if any
			for (int i = startByteIndex + 1; i < endByteIndex; i++)
				put(i, 0);

			// Handle last word
			put(endByteIndex, byt(endByteIndex) & ~lastByteMask);
		}

		recalculateBytesInUse();
	}

	/*--------------------------------------------------------------------------------
	 *  next/previous set/clear bit
	 *-------------------------------------------------------------------------------*/
	/**
	 * Returns the index of the first bit that is set to {@code true} that occurs on
	 * or after the specified starting index. If no such bit exists then {@code -1}
	 * is returned.
	 *
	 * @param fromIndex the index to start checking from (inclusive)
	 * @return the index of the next set bit, or {@code -1} if there is no such bit
	 * @throws IndexOutOfBoundsException if the specified index is negative
	 */
	public int nextSetBit(int fromIndex) {
		if (fromIndex < 0)
			throw new IndexOutOfBoundsException("fromIndex < 0: " + fromIndex);

		final int position = buffer.position();

		int u = byteIndex(fromIndex);
		if (u >= position)
			return -1;

		byte b = (byte) (byt(u) & (MASK << (fromIndex & 7)));

		while (true) {
			if (b != 0)
				return (u * 8) + Integer.numberOfTrailingZeros(b);
			if (++u == position)
				return -1;
			b = byt(u);
		}
	}

	/**
	 * Returns the index of the first bit that is set to {@code false} that occurs
	 * on or after the specified starting index.
	 *
	 * @param fromIndex the index to start checking from (inclusive)
	 * @return the index of the next clear bit
	 * @throws IndexOutOfBoundsException if the specified index is negative
	 */
	public int nextClearBit(int fromIndex) {

		if (fromIndex < 0)
			throw new IndexOutOfBoundsException("fromIndex < 0: " + fromIndex);

		final int position = buffer.position();

		int u = byteIndex(fromIndex);
		if (u >= position)
			return fromIndex;

		byte b = (byte) (~byt(u) & (MASK << (fromIndex & 7)));

		while (true) {
			if (b != 0)
				return (u * 8) + Integer.numberOfTrailingZeros(b);
			if (++u == position)
				return position * 8;
			b = (byte) ~byt(u);
		}
	}

	/**
	 * Returns the index of the nearest bit that is set to {@code true} that occurs
	 * on or before the specified starting index. If no such bit exists, or if
	 * {@code -1} is given as the starting index, then {@code -1} is returned.
	 *
	 * @param fromIndex the index to start checking from (inclusive)
	 * @return the index of the previous set bit, or {@code -1} if there is no such
	 *         bit
	 * @throws IndexOutOfBoundsException if the specified index is less than
	 *                                   {@code -1}
	 */
	public int previousSetBit(int fromIndex) {
		if (fromIndex < 0) {
			if (fromIndex == -1)
				return -1;
			throw new IndexOutOfBoundsException("fromIndex < -1: " + fromIndex);
		}

		int u = byteIndex(fromIndex);
		if (u >= buffer.position())
			return lastSetBit();

		byte b = (byte) (byt(u) & (MASK >>> ((-(fromIndex + 1)) & 7)));

		while (true) {
			if (b != 0)
				return (u + 1) * 8 - 1 - numberOfLeadingZeros(b);
			if (u-- == 0)
				return -1;
			b = byt(u);
		}
	}

	/**
	 * Returns the index of the nearest bit that is set to {@code false} that occurs
	 * on or before the specified starting index. If no such bit exists, or if
	 * {@code -1} is given as the starting index, then {@code -1} is returned.
	 *
	 * @param fromIndex the index to start checking from (inclusive)
	 * @return the index of the previous clear bit, or {@code -1} if there is no
	 *         such bit
	 * @throws IndexOutOfBoundsException if the specified index is less than
	 *                                   {@code -1}
	 */
	public int previousClearBit(int fromIndex) {
		if (fromIndex < 0) {
			if (fromIndex == -1)
				return -1;
			throw new IndexOutOfBoundsException("fromIndex < -1: " + fromIndex);
		}

		int u = byteIndex(fromIndex);
		if (u >= buffer.position())
			return fromIndex;

		byte b = (byte) (~byt(u) & (MASK >>> ((-(fromIndex + 1)) & 7)));

		while (true) {
			if (b != 0)
				return (u + 1) * 8 - 1 - numberOfLeadingZeros(b);
			if (u-- == 0)
				return -1;
			b = (byte) ~byt(u);
		}
	}

	/**
	 * Returns the index of the highest set bit in the bitset, or -1 if the bitset
	 * contains no set bits.
	 *
	 * @return the index of the highest set bit in the bitset, or -1 if the bitset
	 *         contains no set bits.
	 */
	public int lastSetBit() {

		if (isEmpty())
			return -1;

		final int lastUsedIndex = buffer.position() - 1;

		return 8 * lastUsedIndex - 1 + (8 - numberOfLeadingZeros(byt(lastUsedIndex)));
	}

	/*--------------------------------------------------------------------------------
	 *  Logical operations - and/or/xor/andNot
	 *-------------------------------------------------------------------------------*/
	/**
	 * Performs a logical <b>AND</b> of this target bitset with the argument bitset.
	 * This bitset is modified so that each bit in it has the value {@code true} if
	 * and only if it both initially had the value {@code true} and the
	 * corresponding bit in the bitset argument also had the value {@code true}.
	 *
	 * @param set - a {@link BufferBitSet}
	 */
	public void and(BufferBitSet set) {
		if (this == set)
			return;

		int position = buffer.position();
		final int setPosition = set.buffer.position();

		while (position > setPosition)
			put(--position, 0);

		buffer.position(position);

		// Perform logical AND on words in common
		for (int i = 0; i < position; i++)
			put(i, byt(i) & set.byt(i));

		recalculateBytesInUse();
	}

	/**
	 * Performs a logical <b>OR</b> of this bitset with the bitset argument. This
	 * bitset is modified so that a bit in it has the value {@code true} if and only
	 * if it either already had the value {@code true} or the corresponding bit in
	 * the bitset argument has the value {@code true}.
	 *
	 * @param set - a {@link BufferBitSet}
	 */
	public void or(BufferBitSet set) {
		if (this == set)
			return;

		int bytesInCommon = Math.min(this.buffer.position(), set.buffer.position());

		// Perform logical OR on bytes in common
		for (int i = 0; i < bytesInCommon; i++)
			put(i, byt(i) | set.byt(i));

		copyRemainingBytes(bytesInCommon, set);

		// recalculateBytesInUse() is unnecessary
	}

	/**
	 * Performs a logical <b>XOR</b> of this bitset with the bitset argument. This
	 * bitset is modified so that a bit in it has the value {@code true} if and only
	 * if one of the following statements holds:
	 * <ul>
	 * <li>The bit initially has the value {@code true}, and the corresponding bit
	 * in the argument has the value {@code false}.
	 * <li>The bit initially has the value {@code false}, and the corresponding bit
	 * in the argument has the value {@code true}.
	 * </ul>
	 *
	 * @param set - a {@link BufferBitSet}
	 */
	public void xor(BufferBitSet set) {

		int bytesInCommon = Math.min(this.buffer.position(), set.buffer.position());

		// Perform logical XOR on bytes in common
		for (int i = 0; i < bytesInCommon; i++)
			put(i, byt(i) ^ set.byt(i));

		copyRemainingBytes(bytesInCommon, set);

		recalculateBytesInUse();
	}

	/**
	 * Clears all of the bits in this bitset whose corresponding bit is set in the
	 * specified bitset.
	 *
	 * @param set - the {@link BufferBitSet} with which to mask this bitset
	 */
	public void andNot(BufferBitSet set) {

		int bytesInCommon = Math.min(this.buffer.position(), set.buffer.position());

		// Perform logical (a & !b) on bytes in common
		for (int i = bytesInCommon - 1; i >= 0; i--)
			put(i, byt(i) & ~set.byt(i));

		recalculateBytesInUse();
	}

	/*--------------------------------------------------------------------------------
	 *  shift-right
	 *-------------------------------------------------------------------------------*/
	/**
	 * Returns a copy of this bitset with each bit shifted right by {@code offset}.
	 * The resulting bitset will always be stored in newly allocated space, and will
	 * have the same resizable setting as this bitset.
	 * 
	 * @param offset - number of bits to shift by
	 * 
	 * @return a new bitset with shifted right by {@code offset}
	 * 
	 * @throws IllegalArgumentException if offset is negative
	 * @throws IllegalStateException    if the shifted size exceeds the maximum
	 *                                  addressable size ({@code 2^31-1})
	 */
	public BufferBitSet shiftRight(int offset) {

		final int shiftedLastSetBit = lastSetBit() + offset;

		if (offset < 0)
			throw new IllegalArgumentException("offset < 0: " + offset);
		else if (offset == 0 || isEmpty())
			return copy();
		else if (shiftedLastSetBit < 0)
			throw new IllegalStateException("shifted size exceeds max addressable size (2^31-1)");

		final int offsetBits = offset & 7;
		final int offsetBytes = offset >>> 3;

		ByteBuffer buffer = allocate((shiftedLastSetBit >>> 3) + 1);
		buffer.position(offsetBytes);

		if (offsetBits == 0) { // byte aligned
			ByteBuffer from = this.buffer.duplicate();
			from.flip();
			buffer.put(from);
		} else {
			// first byte
			buffer.put((byte) (byt(0) << offsetBits));

			// remaining bytes
			if (buffer.position() < buffer.limit()) {
				LeftShift shift = new LeftShift(8 - offsetBits, lastSetBit() + 1);
				for (int i = 0; i < shift.byteCount; i++)
					buffer.put((byte) shift.get(i));
			}
		}

		return new BufferBitSet(buffer, resizable, false);
	}

	/*--------------------------------------------------------------------------------
	 *  Object & Collection-like methods
	 *-------------------------------------------------------------------------------*/
	/**
	 * Returns a string representation of this {@link BufferBitSet} equivalent to
	 * the representation of a {@code SortedSet} containing the indices of the bits
	 * which are set in this bitset.
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append('[');

		final int position = buffer.position();

		for (int i = 0; i < position; i++) {
			int b = byt(i) & 0xFF;
			for (int j = 0; b > 0; b >>= 1, j++) {
				if ((b & 1) != 0) {
					sb.append((i << 3) + j);
					sb.append(", ");
				}
			}
		}

		if (sb.length() > 1)
			sb.delete(sb.length() - 2, sb.length());

		sb.append(']');
		return sb.toString();
	}

	/**
	 * Returns the number of bits of space actually in use by this
	 * {@link BufferBitSet} to represent bit values. The maximum element that can be
	 * set without resizing is {@code size()-1}
	 *
	 * @return the number of bits of space currently in this bit set
	 */
	public int size() {
		return buffer.limit() * 8;
	}

	/**
	 * Returns true if this {@link BufferBitSet} contains no bits that are set to
	 * {@code true}.
	 *
	 * @return boolean indicating whether this bitset is empty
	 */
	public boolean isEmpty() {
		return buffer.position() == 0;
	}

	/**
	 * Returns the number of bits set to {@code true} in this {@link BufferBitSet}.
	 *
	 * @return the number of bits set to {@code true} in this {@link BufferBitSet}
	 */
	public int cardinality() {

		final int position = buffer.position();
		int count = 0;

		for (int i = 0; i < position; i++)
			count += bitCount(byt(i) & 0xFF);

		return count;
	}

	/**
	 * Returns the number of bits set to true within the given range.
	 *
	 * @param fromIndex - index of the first bit in the range
	 * @param toIndex   - index after the last bit in the range
	 * @return the number of bits set to true within the given range.
	 * @throws IndexOutOfBoundsException if {@code fromIndex} is negative, or
	 *                                   {@code toIndex} is negative, or
	 *                                   {@code fromIndex} is larger than
	 *                                   {@code toIndex}
	 */
	public int cardinality(int fromIndex, int toIndex) {
		checkRange(fromIndex, toIndex);

		int lastSetBit = lastSetBit();

		// If no set bits in range return empty bitset
		if (lastSetBit < fromIndex || fromIndex == toIndex)
			return 0;
		else if (toIndex - 1 > lastSetBit)
			toIndex = lastSetBit + 1;

		int startByteIndex = byteIndex(fromIndex);
		int endByteIndex = byteIndex(toIndex - 1);

		int firstByteMask = (MASK << (fromIndex & 7)) & 0xFF;
		int lastByteMask = MASK >>> ((-toIndex) & 7);

		if (startByteIndex == endByteIndex) {
			// Case 1: One word
			return bitCount(byt(startByteIndex) & (firstByteMask & lastByteMask));
		} else {
			// Case 2: Multiple words
			int count = 0;

			// Handle first word
			count += bitCount(byt(startByteIndex) & firstByteMask);

			// Handle intermediate words, if any
			for (int i = startByteIndex + 1; i < endByteIndex; i++)
				count += bitCount(byt(i) & 0xFF);

			// Handle last word
			count += bitCount(byt(endByteIndex) & lastByteMask);

			return count;
		}
	}

	/**
	 * Returns the hashcode value for this bitset. The hashcode depends only on
	 * which bits are set within this {@link BufferBitSet}.
	 * <p>
	 * Hashcode is computed using formula from
	 * {@link java.util.Arrays#hashCode(byte[])}
	 *
	 * @return the hashcode value for this bitset
	 */
	@Override
	public int hashCode() {

		if (isEmpty())
			return 0;

		final int position = buffer.position();
		int result = 1;

		for (int i = 0; i < position; i++)
			result = 31 * result + byt(i);

		return result;
	}

	/**
	 * Compares this object against the specified object. The result is {@code true}
	 * if and only if the argument is not {@code null} and is a {@code BufferBitset}
	 * object that has exactly the same set of bits set to {@code true} as this bit
	 * set. That is, for every nonnegative {@code int} index {@code k},
	 * 
	 * <pre>
	 * ((BitBufferSet) obj).get(k) == this.get(k)
	 * </pre>
	 * 
	 * must be true. The current sizes of the two bit sets are not compared.
	 *
	 * @param obj the object to compare with
	 * 
	 * @return {@code true} if the objects are the same; {@code false} otherwise
	 */
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof BufferBitSet))
			return false;
		if (this == obj)
			return true;

		final BufferBitSet set = (BufferBitSet) obj;
		final int position = buffer.position();

		if (position != set.buffer.position())
			return false;

		// Check bytes in use by both bitsets
		for (int i = 0; i < position; i++)
			if (byt(i) != set.byt(i))
				return false;

		return true;
	}

	/**
	 * Cloning this bitset produces a new bitset that is equal to it.
	 *
	 * @return another bitset that has exactly the same bits set to {@code true} as
	 *         this one
	 */
	@Override
	public Object clone() {
		return copy();
	}

	/**
	 * Identical to {@link #clone()}, except returns a {@code BufferBitSet} instead
	 * of {@code Object}.
	 * 
	 * @return another bitset that has exactly the same bits set to {@code true} as
	 *         this one
	 */
	public BufferBitSet copy() {

		ByteBuffer copy = BufferUtils.copy(buffer, 0, buffer.position());
		copy.position(buffer.position());

		return new BufferBitSet(copy, resizable, false);
	}

	/*--------------------------------------------------------------------------------
	 *  Methods related to resizing
	 *-------------------------------------------------------------------------------*/
	/**
	 * Ensures that the bitset can accommodate a given wordIndex.
	 */
	private void expandTo(int byteIndex) {

		if (byteIndex >= buffer.limit()) {
			if (!resizable)
				throw new IndexOutOfBoundsException("could not resize to accomodate byte index: " + byteIndex);

			// allocate new buffer
			int capacity = Math.max(buffer.limit() * 2, byteIndex + 1);
			capacity = Math.min(capacity, MAX_CAPACITY);

			final ByteBuffer buffer = allocate(capacity);

			// copy old buffer and replace with new one
			this.buffer.flip();
			this.buffer = buffer.put(this.buffer);
		}

		if (byteIndex >= buffer.position())
			buffer.position(byteIndex + 1);
	}

	/**
	 * Discard upper bytes that are not in use (zero / all clear)
	 */
	private void recalculateBytesInUse() {
		// find last set bit
		int n = buffer.position() - 1;
		while (n >= 0 && byt(n) == 0)
			n--;

		buffer.position(n + 1);
	}

	/**
	 * Bulk copy all bytes from the provided bitset on or after index
	 * {@code bytesInCommon}
	 */
	private void copyRemainingBytes(int bytesInCommon, BufferBitSet set) {
		// Copy any remaining bytes
		if (bytesInCommon < set.buffer.position()) {
			expandTo(set.buffer.position() - 1);

			buffer.position(bytesInCommon);
			buffer.put(slice(set.buffer, bytesInCommon, set.buffer.position()));
		}
	}

	/*--------------------------------------------------------------------------------
	 *  Utility methods
	 *-------------------------------------------------------------------------------*/
	/**
	 * Given a bit index, return byte index containing it.
	 */
	private static int byteIndex(int bitIndex) {
		return bitIndex >> 3;
	}

	/**
	 * Given a bit index, return single-bit mask into containing byte.
	 */
	private static int bit(int bitIndex) {
		return 1 << (bitIndex & 7);
	}

	/**
	 * Given a byte index, return byte value from buffer
	 */
	private byte byt(int byteIndex) {
		return buffer.get(byteIndex);
	}

	// canary method used to detect spurious down-conversions from int to byte
//	private void put(int byteIndex, byte b) {}

	/**
	 * Write a byte to the buffer at the given index. In practice, the "byte" always
	 * comes in as an {@code int} due to widening from logical operations.
	 */
	private void put(int byteIndex, int b) {
		buffer.put(byteIndex, (byte) b);
	}

	/**
	 * Checks that fromIndex ... toIndex is a valid range of bit indices.
	 */
	private static void checkRange(int fromIndex, int toIndex) {
		if (fromIndex < 0)
			throw new IndexOutOfBoundsException("fromIndex < 0: " + fromIndex);
		if (toIndex < 0)
			throw new IndexOutOfBoundsException("toIndex < 0: " + toIndex);
		if (fromIndex > toIndex)
			throw new IndexOutOfBoundsException("fromIndex: " + fromIndex + " > toIndex: " + toIndex);
	}

	/**
	 * {@link Integer#numberOfLeadingZeros(int)} modified to work for a {@code byte}
	 */
	private static int numberOfLeadingZeros(byte b) {
		return Integer.numberOfLeadingZeros(b & 0xFF) - 24;
	}
}
