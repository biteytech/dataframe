/*
 * Copyright 2019 biteytech@protonmail.com
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
import static tech.bitey.dataframe.StringColumn.UTF_8;
import static tech.bitey.dataframe.guava.DfPreconditions.checkArgument;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Spliterator;

import tech.bitey.bufferstuff.BufferBitSet;
import tech.bitey.bufferstuff.BufferUtils;

/**
 * A builder for creating {@link StringColumn} instances. Example:
 *
 * <pre>
 * StringColumn column = StringColumn.builder().add("Hello").add("World", "!").build();
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
public final class StringColumnBuilder extends AbstractColumnBuilder<String, StringColumn, StringColumnBuilder> {

	StringColumnBuilder(int characteristics) {
		super(characteristics);
	}

	private final ArrayList<String> elements = new ArrayList<>();

	@Override
	void addNonNull(String element) {
		elements.add(element);
		size++;
	}

	@Override
	void ensureAdditionalCapacity(int size) {
		elements.ensureCapacity(elements.size() + size);
	}

	@Override
	public StringColumnBuilder ensureCapacity(int minCapacity) {
		elements.ensureCapacity(minCapacity);
		return this;
	}

	@Override
	StringColumn emptyNonNull() {
		return NonNullStringColumn.EMPTY.get(characteristics | Spliterator.NONNULL);
	}

	@Override
	int getNonNullSize() {
		return elements.size();
	}

	@Override
	void checkCharacteristics() {
		if (elements.size() >= 2) {
			String prev = elements.get(0);

			if ((characteristics & DISTINCT) != 0) {
				for (int i = 1; i < elements.size(); i++) {
					String e = elements.get(i);
					checkArgument(prev.compareTo(e) < 0, "column elements must be sorted and distinct");
					prev = e;
				}
			} else if ((characteristics & SORTED) != 0) {
				for (int i = 1; i < elements.size(); i++) {
					String e = elements.get(i);
					checkArgument(prev.compareTo(e) <= 0, "column elements must be sorted");
					prev = e;
				}
			}
		}
	}

	@Override
	StringColumn buildNonNullColumn(int characteristics) {

		int byteLength = 0;
		for (int i = 0; i < elements.size(); i++)
			byteLength += elements.get(i).getBytes(UTF_8).length;

		ByteBuffer elements = BufferUtils.allocate(byteLength);
		ByteBuffer pointers = BufferUtils.allocate(this.elements.size() * 4);

		int destPos = 0;
		for (String e : this.elements) {
			byte[] bytes = e.getBytes(UTF_8);
			elements.put(bytes);
			pointers.putInt(destPos);
			destPos += bytes.length;
		}
		pointers.flip();
		elements.flip();

		return new NonNullStringColumn(elements, pointers, 0, this.elements.size(), characteristics, false);
	}

	@Override
	StringColumn wrapNullableColumn(StringColumn column, BufferBitSet nonNulls) {
		return new NullableStringColumn((NonNullStringColumn) column, nonNulls, null, 0, size);
	}

	@Override
	public ColumnType getType() {
		return ColumnType.STRING;
	}

	@Override
	void append0(StringColumnBuilder tail) {
		this.elements.addAll(tail.elements);
	}

	void sort() {
		Collections.sort(elements);
		characteristics |= SORTED;
	}
}
