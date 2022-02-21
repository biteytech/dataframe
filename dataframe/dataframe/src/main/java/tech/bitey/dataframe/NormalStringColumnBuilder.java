/*
 * Copyright 2022 biteytech@protonmail.com
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.bitey.dataframe;

import static java.util.Spliterator.NONNULL;

import java.util.HashMap;
import java.util.Map;

import tech.bitey.bufferstuff.BufferBitSet;

/**
 * A builder for creating {@link NormalStringColumn} instances. Example:
 *
 * <pre>
 * NormalStringColumn column = NormalStringColumn.builder().add("Hello").add("World", "!").build();
 * </pre>
 * 
 * Elements appear in the resulting column in the same order they were added to
 * the builder.
 * <p>
 * Builder instances can be reused; it is safe to call
 * {@link ColumnBuilder#build build} multiple times to build multiple columns in
 * series. Each new column contains all the elements of the ones created before
 * it.
 *
 * @author biteytech@protonmail.com
 */
public final class NormalStringColumnBuilder
		extends AbstractColumnBuilder<String, NormalStringColumn, NormalStringColumnBuilder> {

	private final ByteColumnBuilder builder;
	private final StringColumnBuilder values;

	private final Map<NormalStringHash, Byte> codeMap;

	NormalStringColumnBuilder() {
		super(0);

		builder = new ByteColumnBuilder(NONNULL);
		values = new StringColumnBuilder(NONNULL);

		codeMap = new HashMap<>();
	}

	@Override
	public ColumnType<String> getType() {
		return ColumnType.NSTRING;
	}

	@Override
	void addNonNull(String element) {

		int mapSize = codeMap.size();

		byte b = codeMap.computeIfAbsent(new NormalStringHash(element), x -> {
			if (mapSize == 256)
				throw new RuntimeException("exceeded 256 distinct values");
			values.add(element);
			return (byte) mapSize;
		});

		builder.add(b);
		size++;
	}

	@Override
	public ColumnBuilder<String> ensureCapacity(int minCapacity) {
		builder.ensureCapacity(minCapacity);
		return this;
	}

	@Override
	int getNonNullSize() {
		return builder.getNonNullSize();
	}

	@Override
	void ensureAdditionalCapacity(int additionalCapacity) {
		builder.ensureCapacity(builder.size() + additionalCapacity);
	}

	@Override
	int compareToLast(String element) {
		throw new UnsupportedOperationException("compareToLast");
	}

	@Override
	void checkCharacteristics() {
		throw new UnsupportedOperationException("checkCharacteristics");
	}

	@Override
	CharacteristicValidation getCharacteristicValidation() {
		return CharacteristicValidation.NONE;
	}

	@Override
	NormalStringColumn emptyNonNull() {
		return NormalStringColumnImpl.EMPTY;
	}

	@Override
	NormalStringColumn buildNonNullColumn(int characteristics) {

		return new NormalStringColumnImpl(builder.build(), (NonNullStringColumn) values.build(), 0, builder.size());
	}

	@Override
	NormalStringColumn wrapNullableColumn(NormalStringColumn column, BufferBitSet nonNulls) {

		NormalStringColumnImpl impl = (NormalStringColumnImpl) column;

		ByteColumn bytes = new NullableByteColumn((NonNullByteColumn) impl.bytes, nonNulls, null, 0, size);

		return new NormalStringColumnImpl(bytes, impl.values, 0, size);
	}

	@Override
	void append0(NormalStringColumnBuilder tail) {

		int headSize = builder.size();
		int tailSize = tail.builder.size();

		builder.append(tail.builder);

		byte[] remap = new byte[tail.codeMap.size()];
		int r = codeMap.size();
		IntColumnBuilder indices = IntColumn.builder(NONNULL);
		for (var e : tail.codeMap.entrySet()) {

			int tailByte = e.getValue() & 0xFF;

			if (codeMap.containsKey(e.getKey())) {
				remap[tailByte] = codeMap.get(e.getKey());
			} else {
				if (r >= 256)
					throw new RuntimeException("exceeded 256 distinct values");

				codeMap.put(e.getKey(), remap[tailByte] = (byte) (r++));
				indices.add(tailByte);
			}
		}

		values.addAll(((NonNullStringColumn) tail.values.build()).select(indices.build()));

		for (int i = headSize; i < headSize + tailSize; i++) {
			byte b = builder.buffer.get(i);
			builder.buffer.put(i, remap[b & 0xFF]);
		}
	}
}
