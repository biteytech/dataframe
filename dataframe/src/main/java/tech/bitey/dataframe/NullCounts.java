package tech.bitey.dataframe;

import java.nio.ByteBuffer;

import tech.bitey.bufferstuff.BufferBitSet;
import tech.bitey.bufferstuff.BufferUtils;

class NullCounts {

	final BufferBitSet nonNulls;
	final ByteBuffer bb;
	final boolean isShort;

	NullCounts(BufferBitSet nonNulls, int size) {

		this.nonNulls = nonNulls;
		isShort = size <= Short.MAX_VALUE;

		final int words = (size - 1) / 32 + 1;

		bb = BufferUtils.allocate((words + 1) * (isShort ? 2 : 4));

		if (isShort)
			bb.putShort(0, (short) 0);
		else
			bb.putInt(0, 0);

		for (int i = 0, w = 1, count = 0; w <= words; w++) {
			count += 32 - nonNulls.cardinality(i, i += 32);
			if (isShort)
				bb.putShort(w << 1, (short) count);
			else
				bb.putInt(w << 2, count);
		}
	}

	int nonNullIndex(int index) {
		// count null bits before index
		final int word = (index - 1) / 32;
		final int from = word << 5;
		final int nulls = isShort ? bb.getShort(word << 1) : bb.getInt(word << 2);

		return from + nonNulls.cardinality(from, index) - nulls;
	}
}
