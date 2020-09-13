/*
 * Copyright 2020 biteytech@protonmail.com
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

import java.util.Iterator;

class DataFrameBackedMap<K> extends AbstractKeyBackedMap<K, Row> {

	private final DataFrame dataFrame;

	DataFrameBackedMap(DataFrame dataFrame) {
		super(dataFrame.column(dataFrame.keyColumnIndex()));

		this.dataFrame = dataFrame;
	}

	@Override
	public DataFrame values() {
		return dataFrame;
	}

	@Override
	public Row get(Object key) {

		int index = keyColumn.indexOf(key);

		return index >= 0 ? dataFrame.get(index) : null;
	}

	@Override
	Entry<K, Row> entry(int index) {
		return new DfEntry(index);
	}

	private class DfEntry implements Entry<K, Row> {

		private final int index;

		private DfEntry(int index) {
			this.index = index;
		}

		@Override
		public K getKey() {
			return keyColumn.get(index);
		}

		@Override
		public Row getValue() {
			return dataFrame.get(index);
		}

		@Override
		public Row setValue(Row value) {
			throw new UnsupportedOperationException("setValue");
		}
	}

	@Override
	public AbstractKeyBackedMap<K, Row> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
		return new DataFrameBackedMap<>(dataFrame.subFrameByValue(fromKey, fromInclusive, toKey, toInclusive));
	}

	@Override
	public AbstractKeyBackedMap<K, Row> headMap(K toKey, boolean inclusive) {
		return new DataFrameBackedMap<>(dataFrame.headTo(toKey, inclusive));
	}

	@Override
	public AbstractKeyBackedMap<K, Row> tailMap(K fromKey, boolean inclusive) {
		return new DataFrameBackedMap<>(dataFrame.tailFrom(fromKey, inclusive));
	}

	@Override
	public AbstractKeyBackedMap<K, Row> subMap(K fromKey, K toKey) {
		return new DataFrameBackedMap<>(dataFrame.subFrameByValue(fromKey, toKey));
	}

	@Override
	public AbstractKeyBackedMap<K, Row> headMap(K toKey) {
		return new DataFrameBackedMap<>(dataFrame.headTo(toKey));
	}

	@Override
	public AbstractKeyBackedMap<K, Row> tailMap(K fromKey) {
		return new DataFrameBackedMap<>(dataFrame.tailFrom(fromKey));
	}

	@Override
	Iterator<Row> descendingValuesIterator() {
		return ColumnBackedSet.descendingIterator(dataFrame);
	}

	@Override
	int valuesCharacteristics() {
		return dataFrame.characteristics();
	}

}
