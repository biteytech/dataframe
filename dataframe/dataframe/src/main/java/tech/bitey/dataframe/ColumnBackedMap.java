/*
 * Copyright 2021 biteytech@protonmail.com
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

import static tech.bitey.dataframe.Pr.checkArgument;
import static tech.bitey.dataframe.Pr.checkNotNull;

import java.util.Iterator;

class ColumnBackedMap<K extends Comparable<? super K>, V extends Comparable<? super V>>
		extends AbstractKeyBackedMap<K, V> {

	private final Column<V> valueColumn;

	ColumnBackedMap(Column<K> keyColumn, Column<V> valueColumn) {
		super(keyColumn);

		checkNotNull(keyColumn, "key column cannot be null");
		checkNotNull(valueColumn, "value column cannot be null");
		checkArgument(keyColumn.isDistinct(), "key column must be a unique index");
		checkArgument(keyColumn.size() == valueColumn.size(), "key and value columns must have the same size");

		this.valueColumn = valueColumn;
	}

	@Override
	public V get(Object o) {
		int index = keyColumn.indexOf(o);
		return index < 0 ? null : valueColumn.get(index);
	}

	@Override
	public boolean containsValue(Object o) {
		return valueColumn.contains(o);
	}

	@Override
	public Column<V> values() {
		return valueColumn;
	}

	@Override
	Entry<K, V> entry(int index) {
		return new ColumnEntry(index);
	}

	private class ColumnEntry extends AbstractEntry<K, V> {

		private ColumnEntry(int index) {
			super(index);
		}

		@Override
		public K getKey() {
			return keyColumn.get(index);
		}

		@Override
		public V getValue() {
			return valueColumn.get(index);
		}
	}

	@Override
	public AbstractKeyBackedMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {

		NonNullColumn<K, ?, ?> subKeyColumn = keyColumn.subColumnByValue(fromKey, fromInclusive, toKey, toInclusive);

		return subMap(subKeyColumn);
	}

	private AbstractKeyBackedMap<K, V> subMap(NonNullColumn<K, ?, ?> subKeyColumn) {

		if (subKeyColumn.isEmpty())
			return new ColumnBackedMap<>(subKeyColumn, valueColumn.subColumn(0, 0));

		int idx = keyColumn.indexOf(subKeyColumn.first());
		return new ColumnBackedMap<>(subKeyColumn, valueColumn.subColumn(idx, idx + subKeyColumn.size()));
	}

	@Override
	public AbstractKeyBackedMap<K, V> headMap(K toKey, boolean inclusive) {

		NonNullColumn<K, ?, ?> subKeyColumn = keyColumn.head(toKey, inclusive);

		return subMap(subKeyColumn);
	}

	@Override
	public AbstractKeyBackedMap<K, V> tailMap(K fromKey, boolean inclusive) {

		NonNullColumn<K, ?, ?> subKeyColumn = keyColumn.tail(fromKey, inclusive);

		return subMap(subKeyColumn);
	}

	@Override
	public AbstractKeyBackedMap<K, V> subMap(K fromKey, K toKey) {

		NonNullColumn<K, ?, ?> subKeyColumn = keyColumn.subColumnByValue(fromKey, toKey);

		return subMap(subKeyColumn);
	}

	@Override
	public AbstractKeyBackedMap<K, V> headMap(K toKey) {

		NonNullColumn<K, ?, ?> subKeyColumn = keyColumn.head(toKey);

		return subMap(subKeyColumn);
	}

	@Override
	public AbstractKeyBackedMap<K, V> tailMap(K fromKey) {

		NonNullColumn<K, ?, ?> subKeyColumn = keyColumn.tail(fromKey);

		return subMap(subKeyColumn);
	}

	@Override
	Iterator<V> descendingValuesIterator() {
		return ColumnBackedSet.descendingIterator(valueColumn);
	}

	@Override
	int valuesCharacteristics() {
		return valueColumn.characteristics();
	}
}
