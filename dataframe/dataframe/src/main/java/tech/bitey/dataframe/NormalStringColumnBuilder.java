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

	static final int MAX_VALUES = 1 << 16;
	private static final String MAX_VALUE_ERROR = "exceeded %d distinct values".formatted(MAX_VALUES);

	private final ShortColumnBuilder builder;
	private final StringColumnBuilder values;

	private final Map<NormalStringHash, Integer> valueIndexMap;

	NormalStringColumnBuilder() {
		super(0);

		builder = new ShortColumnBuilder(NONNULL);
		values = new StringColumnBuilder(NONNULL);

		valueIndexMap = new HashMap<>();
	}

	@Override
	public ColumnType<String> getType() {
		return ColumnType.NSTRING;
	}

	@Override
	void addNonNull(String element) {

		int mapSize = valueIndexMap.size();

		int index = valueIndexMap.computeIfAbsent(new NormalStringHash(element), x -> {
			if (mapSize == MAX_VALUES)
				throw new RuntimeException(MAX_VALUE_ERROR);
			values.add(element);
			return mapSize;
		});

		builder.add((short) index);
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
		return NormalStringColumnByteImpl.EMPTY;
	}

	@Override
	NormalStringColumn buildNonNullColumn(int characteristics) {

		if (valueIndexMap.size() <= 256)
			return new NormalStringColumnByteImpl(builder.build().toByteColumn(Short::byteValue),
					(NonNullStringColumn) values.build(), 0, builder.size());
		else
			return new NormalStringColumnShortImpl(builder.build(), (NonNullStringColumn) values.build(), 0,
					builder.size());
	}

	@Override
	NormalStringColumn wrapNullableColumn(NormalStringColumn column, BufferBitSet nonNulls) {

		if (column instanceof NormalStringColumnByteImpl impl) {
			ByteColumn bytes = new NullableByteColumn((NonNullByteColumn) impl.indices, nonNulls, null, 0, size);
			return new NormalStringColumnByteImpl(bytes, impl.values, 0, size);
		} else {
			NormalStringColumnShortImpl impl = (NormalStringColumnShortImpl) column;
			ShortColumn shorts = new NullableShortColumn((NonNullShortColumn) impl.indices, nonNulls, null, 0, size);
			return new NormalStringColumnShortImpl(shorts, impl.values, 0, size);
		}
	}

	@Override
	void append0(NormalStringColumnBuilder tail) {

		int headSize = builder.size();
		int tailSize = tail.builder.size();

		builder.append(tail.builder);

		short[] remap = new short[tail.valueIndexMap.size()];
		int r = valueIndexMap.size();
		IntColumnBuilder indices = IntColumn.builder(NONNULL);
		for (var e : tail.valueIndexMap.entrySet()) {

			int tailIndex = e.getValue();

			if (valueIndexMap.containsKey(e.getKey())) {
				remap[tailIndex] = valueIndexMap.get(e.getKey()).shortValue();
			} else {
				if (r >= MAX_VALUES)
					throw new RuntimeException(MAX_VALUE_ERROR);

				remap[tailIndex] = (short) (r);
				valueIndexMap.put(e.getKey(), r);
				r++;
				indices.add(tailIndex);
			}
		}

		values.addAll(((NonNullStringColumn) tail.values.build()).select(indices.build()));

		for (int i = headSize; i < headSize + tailSize; i++) {
			short s = builder.buffer.getShort(i * 2);
			builder.buffer.putShort(i * 2, remap[s & 0xFFFF]);
		}
	}
}
