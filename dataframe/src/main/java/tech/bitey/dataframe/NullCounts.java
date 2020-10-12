package tech.bitey.dataframe;

import java.nio.ByteBuffer;
import java.util.function.IntUnaryOperator;

import tech.bitey.bufferstuff.BufferBitSet;
import tech.bitey.bufferstuff.BufferUtils;

class NullCounts implements INullCounts {

	@FunctionalInterface
	private interface IntBiConsumer {
		void accept(int value1, int value2);
	}

	private final BufferBitSet nonNulls;
	private final IntUnaryOperator get;

	NullCounts(BufferBitSet nonNulls, int size) {

		this.nonNulls = nonNulls;

		final int cardinality = size - nonNulls.cardinality();

		final int countByteSize;
		if (cardinality < 1 << 8)
			countByteSize = 1;
		else if (cardinality < 1 << 16)
			countByteSize = 2;
		else
			countByteSize = 4;

		final int words = (size - 1) / WORD_SIZE + 1;

		final ByteBuffer bb = BufferUtils.allocate((words + 1) * countByteSize);

		final IntBiConsumer put;
		if (cardinality < 1 << 8) {
			put = (w, count) -> bb.put(w, (byte) count);
			get = w -> bb.get(w) & 0xFF;
		} else if (cardinality < 1 << 16) {
			put = (w, count) -> bb.putShort(w << 1, (short) count);
			get = w -> bb.getShort(w << 1) & 0xFFFF;
		} else {
			put = (w, count) -> bb.putInt(w << 2, count);
			get = w -> bb.getInt(w << 2);
		}

		put.accept(0, 0);

		for (int i = 0, w = 1, count = 0; w <= words; w++) {
			count += WORD_SIZE - nonNulls.cardinality(i, i += WORD_SIZE);
			put.accept(w, count);
		}
	}

	@Override
	public int nonNullIndex(int index) {
		// count null bits before index
		final int word = (index - 1) / WORD_SIZE;
		final int from = word << WORD_SHIFT;
		final int nulls = get.applyAsInt(word);

		return from + nonNulls.cardinality(from, index) - nulls;
	}
}
