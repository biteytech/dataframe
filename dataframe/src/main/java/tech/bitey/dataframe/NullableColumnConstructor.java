package tech.bitey.dataframe;

import tech.bitey.bufferstuff.BufferBitSet;

@FunctionalInterface
interface NullableColumnConstructor<E, I extends Column<E>, C extends NonNullColumn<E, I, C>, N extends NullableColumn<E, I, C, N>> {

	N create(NonNullColumn<E, I, C> column, BufferBitSet nonNulls, INullCounts nullCounts, int offset, int size);
}
