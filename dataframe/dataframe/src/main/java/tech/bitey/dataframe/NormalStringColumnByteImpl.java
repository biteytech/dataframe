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

import static tech.bitey.dataframe.NonNullColumn.NONNULL_CHARACTERISTICS;

import java.util.List;
import java.util.function.Predicate;

import tech.bitey.bufferstuff.BufferBitSet;

public class NormalStringColumnByteImpl extends NormalStringColumnImpl<Byte, ByteColumn, NormalStringColumnByteImpl>
		implements NormalStringColumn {

	static final NormalStringColumnByteImpl EMPTY = new NormalStringColumnByteImpl(
			NonNullByteColumn.empty(NONNULL_CHARACTERISTICS), NonNullStringColumn.empty(NONNULL_CHARACTERISTICS), 0, 0);

	NormalStringColumnByteImpl(ByteColumn indices, NonNullStringColumn values, int offset, int size) {
		super(indices, values, offset, size);
	}

	@Override
	NormalStringColumnByteImpl constuct(ByteColumn indices, NonNullStringColumn values, int offset, int size) {
		return new NormalStringColumnByteImpl(indices, values, offset, size);
	}

	@Override
	NormalStringColumnByteImpl empty() {
		return EMPTY;
	}

	@Override
	String at(int index) {
		return values.get(indices.get(index) & 0xFF);
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		} else if (o instanceof NormalStringColumnByteImpl) {
			return equals0((NormalStringColumnByteImpl) o);
		} else if (o instanceof List) {
			return super.equals(o);
		} else {
			return false;
		}
	}

	/*------------------------------------------------------------
	 *                      clean & filter
	 *------------------------------------------------------------*/

	private class NSFilter {

		final BufferBitSet filter = new BufferBitSet();
		final int cardinality;

		NSFilter(Predicate<String> predicate) {

			int cardinality = 0;

			for (int i = values.lastIndex(); i >= 0; i--) {
				if (predicate.test(values.get(i))) {
					filter.set(i);
					cardinality++;
				}
			}

			this.cardinality = cardinality;
		}

		NormalStringColumn finish(ByteColumn indices, boolean flip) {

			int cardinality = this.cardinality;

			if (flip) {
				filter.flip(0, values.size());
				cardinality = values.size() - cardinality;
			}

			NonNullStringColumn vals = (NonNullStringColumn) values.applyFilter(filter, cardinality);

			byte[] remap = new byte[values.size()];
			for (int i = 0, j = 0; i < remap.length; i++)
				if (filter.get(i))
					remap[i] = (byte) (j++);

			ByteColumn byts = indices.evaluate(b -> remap[b & 0xFF]);

			return new NormalStringColumnByteImpl(byts, vals, 0, byts.size());
		}
	}

	@Override
	public NormalStringColumn clean(Predicate<String> predicate) {

		NSFilter filter = new NSFilter(predicate);

		if (filter.cardinality == values.size())
			return NormalStringColumn.builder().addNulls(size).build();
		else if (filter.cardinality == 0)
			return this;

		ByteColumn subColumn = this.indices.subColumn(offset, offset + size);
		ByteColumn indices = subColumn.cleanByte(b -> filter.filter.get(b & 0xFF));

		return filter.finish(indices, true);
	}

	@Override
	public NormalStringColumn filter(Predicate<String> predicate, boolean keepNulls) {

		NSFilter filter = new NSFilter(predicate);

		if (indices.isNonnull() || keepNulls) {
			if (filter.cardinality == values.size())
				return this;
			else if (filter.cardinality == 0)
				return EMPTY;
		}

		ByteColumn subColumn = this.indices.subColumn(offset, offset + size);
		ByteColumn indices = subColumn.filterByte(b -> filter.filter.get(b & 0xFF), keepNulls);

		if (indices.size() == 0)
			return EMPTY;
		else
			return filter.finish(indices, false);
	}
}
