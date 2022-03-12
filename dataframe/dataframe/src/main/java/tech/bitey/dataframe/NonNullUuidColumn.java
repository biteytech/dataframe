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

import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.SORTED;
import static tech.bitey.bufferstuff.BufferUtils.readFully;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import tech.bitey.bufferstuff.BufferBitSet;
import tech.bitey.bufferstuff.BufferUtils;

final class NonNullUuidColumn extends NonNullColumn<UUID, UuidColumn, NonNullUuidColumn> implements UuidColumn {

	private static final NonNullLongColumn EMPTY_COLUMN = (NonNullLongColumn) LongColumn.of();

	static final Map<Integer, NonNullUuidColumn> EMPTY = new HashMap<>();
	static {
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS,
				c -> new NonNullUuidColumn(EMPTY_COLUMN, EMPTY_COLUMN, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED,
				c -> new NonNullUuidColumn(EMPTY_COLUMN, EMPTY_COLUMN, 0, 0, c, false));
		EMPTY.computeIfAbsent(NONNULL_CHARACTERISTICS | SORTED | DISTINCT,
				c -> new NonNullUuidColumn(EMPTY_COLUMN, EMPTY_COLUMN, 0, 0, c, false));
	}

	static NonNullUuidColumn empty(int characteristics) {
		return EMPTY.get(characteristics | NONNULL_CHARACTERISTICS);
	}

	private final NonNullLongColumn msb;
	private final NonNullLongColumn lsb;

	NonNullUuidColumn(NonNullLongColumn msb, NonNullLongColumn lsb, int offset, int size, int characteristics,
			boolean view) {
		super(offset, size, characteristics, view);

		this.msb = msb;
		this.lsb = lsb;
	}

	private long msb(int index) {
		return msb.getLong(index);
	}

	private long lsb(int index) {
		return lsb.getLong(index);
	}

	@Override
	public ColumnType<UUID> getType() {
		return ColumnType.UUID;
	}

	private NonNullLongColumn sub(NonNullLongColumn col) {
		return col.subColumn0(offset, offset + size);
	}

	@Override
	public UuidColumn copy() {
		return new NonNullUuidColumn(sub(msb).copy(), sub(lsb).copy(), 0, size, characteristics, false);
	}

	@Override
	NonNullUuidColumn slice() {
		return new NonNullUuidColumn(sub(msb), sub(lsb), 0, size, characteristics, false);
	}

	@Override
	NonNullUuidColumn appendNonNull(NonNullUuidColumn tail) {
		NonNullUuidColumn lhs = this.slice();
		NonNullUuidColumn rhs = tail.slice();

		return new NonNullUuidColumn(lhs.msb.appendNonNull(rhs.msb), lhs.lsb.appendNonNull(rhs.lsb), 0,
				size + tail.size, 0, false);
	}

	@Override
	NonNullUuidColumn subColumn0(int fromIndex, int toIndex) {
		return new NonNullUuidColumn(msb, lsb, fromIndex + offset, toIndex - fromIndex, characteristics, true);
	}

	@Override
	NonNullUuidColumn withCharacteristics(int characteristics) {
		return new NonNullUuidColumn(msb, lsb, offset, size, characteristics, view);
	}

	int search(UUID value) {
		return AbstractColumnSearch.binarySearch(this, offset, offset + size, value);
	}

	@Override
	int search(UUID value, boolean first) {
		return AbstractColumnSearch.search(this, value, first);
	}

	@Override
	boolean checkSorted() {
		if (size < 2)
			return true;

		for (int i = offset + 1; i <= lastIndex(); i++) {
			if (compareValuesAt(i - 1, i) > 0)
				return false;
		}

		return true;
	}

	@Override
	boolean checkDistinct() {
		if (size < 2)
			return true;

		for (int i = offset + 1; i <= lastIndex(); i++) {
			if (compareValuesAt(i - 1, i) >= 0)
				return false;
		}

		return true;
	}

	@Override
	NonNullUuidColumn toSorted0() {
		return NonNullVarLenColumn.toSorted00(this, (l, r) -> compareValuesAt(l + offset, r + offset));
	}

	@Override
	NonNullUuidColumn toDistinct0(boolean sort) {

		NonNullUuidColumn col = this;

		if (sort)
			col = toSorted0();

		return col.toDistinct00();
	}

	NonNullUuidColumn toDistinct00() {

		BufferBitSet keep = new BufferBitSet();
		int cardinality = 0;
		for (int i = lastIndex(); i >= offset;) {

			keep.set(i - offset);
			cardinality++;

			i--;
			for (; i >= offset; i--) {
				if (msb(i) != msb(i + 1) || lsb(i) != lsb(i + 1))
					break;
			}
		}

		NonNullUuidColumn filtered = (NonNullUuidColumn) applyFilter(keep, cardinality);
		return filtered.withCharacteristics(NONNULL_CHARACTERISTICS | SORTED | DISTINCT);
	}

	@Override
	int hashCode(int fromIndex, int toIndex) {
		return msb.hashCode(fromIndex, toIndex) ^ lsb.hashCode(fromIndex, toIndex);
	}

	@Override
	boolean equals0(NonNullUuidColumn rhs) {

		return sub(msb).equals(rhs.sub(rhs.msb)) && sub(lsb).equals(rhs.sub(rhs.lsb));
	}

	@Override
	boolean equals0(NonNullUuidColumn rhs, int lStart, int rStart, int length) {
		// should never be called
		throw new IllegalStateException("equals0");
	}

	@Override
	int compareValuesAt(NonNullUuidColumn rhs, int l, int r) {

		long L = msb.elements.get(l);
		long R = rhs.msb.elements.get(r);

		if (L < R)
			return -1;
		else if (L > R)
			return 1;
		else {
			L = lsb.elements.get(l);
			R = rhs.lsb.elements.get(r);

			if (L < R)
				return -1;
			else if (L > R)
				return 1;
			else
				return 0;
		}
	}

	private int compareValuesAt(int l, int r) {
		return compareValuesAt(this, l, r);
	}

	@Override
	void intersectLeftSorted(NonNullUuidColumn rhs, IntColumnBuilder indices, BufferBitSet keepRight) {

		for (int i = rhs.offset; i <= rhs.lastIndex(); i++) {

			int leftIndex = search(rhs.getNoOffset(i));
			if (leftIndex >= offset && leftIndex <= lastIndex()) {

				indices.add(leftIndex - offset);
				keepRight.set(i - rhs.offset);
			}
		}
	}

	@Override
	NonNullUuidColumn empty() {
		return EMPTY.get(characteristics);
	}

	@Override
	UUID getNoOffset(int index) {
		return new UUID(msb(index), lsb(index));
	}

	@Override
	boolean checkType(Object o) {
		return o instanceof UUID;
	}

	@Override
	Column<UUID> applyFilter0(BufferBitSet keep, int cardinality) {
		return new NonNullUuidColumn(sub(msb).applyFilter0(keep, cardinality), sub(lsb).applyFilter0(keep, cardinality),
				0, cardinality, characteristics, false);
	}

	@Override
	NonNullUuidColumn select0(IntColumn indices) {

		UuidColumnBuilder builder = new UuidColumnBuilder(NONNULL_CHARACTERISTICS);
		builder.ensureCapacity(indices.size());

		for (int i = 0; i < indices.size(); i++) {
			int idx = indices.getInt(i) + offset;
			builder.add(msb(idx), lsb(idx));
		}

		return (NonNullUuidColumn) builder.build();
	}

	@Override
	void writeTo(WritableByteChannel channel) throws IOException {
		sub(msb).writeTo(channel);
		sub(lsb).writeTo(channel);
	}

	@Override
	NonNullUuidColumn readFrom(ReadableByteChannel channel, int version) throws IOException {
		if (version <= 2) {
			ByteOrder order = readByteOrder(channel);
			int size = readInt(channel, order);

			ByteBuffer buffer = BufferUtils.allocate(size * 16, order);
			readFully(channel, buffer);
			buffer.flip();

			UuidColumnBuilder builder = UuidColumn.builder(characteristics);
			builder.ensureCapacity(size);

			LongBuffer longs = buffer.asLongBuffer();
			for (int i = 0; i < size; i++) {
				long msb = longs.get();
				long lsb = longs.get();
				builder.add(msb, lsb);
			}

			return (NonNullUuidColumn) builder.build();

		} else {
			NonNullLongColumn msb = (NonNullLongColumn) ColumnType.LONG.readFrom(channel, NONNULL_CHARACTERISTICS,
					version);
			NonNullLongColumn lsb = (NonNullLongColumn) ColumnType.LONG.readFrom(channel, NONNULL_CHARACTERISTICS,
					version);

			return new NonNullUuidColumn(msb, lsb, 0, msb.size(), characteristics, false);
		}
	}

}
