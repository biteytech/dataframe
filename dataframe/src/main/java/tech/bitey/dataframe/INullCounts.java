package tech.bitey.dataframe;

import tech.bitey.bufferstuff.BufferBitSet;

@FunctionalInterface
interface INullCounts {

	/*
	 * Could be any size. A larger word size means fewer counts (so less extra
	 * memory used to achieve constant-time lookups), but more expensive calls to
	 * nonNulls.cardinality (larger ranges). A size of 32 works out to one bit per
	 * index for large Columns.
	 */
	static final int WORD_SHIFT = 5;
	static final int WORD_SIZE = 1 << WORD_SHIFT;

	int nonNullIndex(int index);

	static INullCounts of(BufferBitSet nonNulls, int size) {

		if (size <= WORD_SIZE)
			return i -> nonNulls.cardinality(0, i);
		else
			return new NullCounts(nonNulls, size);
	}
}
