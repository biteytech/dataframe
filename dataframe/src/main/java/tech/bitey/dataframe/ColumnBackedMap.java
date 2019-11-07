/*
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

import static tech.bitey.dataframe.guava.DfPreconditions.checkArgument;
import static tech.bitey.dataframe.guava.DfPreconditions.checkNotNull;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

class ColumnBackedMap<K, V> extends AbstractMap<K, V> {

	private final Column<K> keyColumn;
	private final Column<V> valueColumn;
	
	ColumnBackedMap(Column<K> keyColumn, Column<V> valueColumn) {
		
		checkNotNull(keyColumn, "key column cannot be null");
		checkNotNull(valueColumn, "value column cannot be null");
		checkArgument(keyColumn.isDistinct(), "key column must be a unique index");
		checkArgument(keyColumn.size() == valueColumn.size(), "key and value columns must have the same size");
		
		this.keyColumn = keyColumn;
		this.valueColumn = valueColumn;
	}
	
	@Override
	public V get(Object o) {
		int index = keyColumn.indexOf(o);
		return index < 0 ? null : valueColumn.get(index);
	}
	
	@Override
	public boolean containsKey(Object o) {
		return keyColumn.indexOf(o) >= 0;
	}
	
	@Override
	public boolean containsValue(Object o) {
		return valueColumn.indexOf(o) >= 0;
	}
	
	@Override
	public int size() {
		return keyColumn.size();
	}
	
	private class ColumnEntry implements Map.Entry<K, V> {

		private final int index;
		
		private ColumnEntry(int index) {
			this.index = index;
		}
		
		@Override
		public K getKey() {
			return keyColumn.get(index);
		}

		@Override
		public V getValue() {
			return valueColumn.get(index);
		}

		@Override
		public V setValue(V value) {
			throw new UnsupportedOperationException("setValue");
		}		
	}
	
	private class ColumnEntrySet extends AbstractSet<Map.Entry<K, V>> {
		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public Iterator<Map.Entry<K, V>> iterator() {
			return new Iterator() {

				int index = 0;
				
				@Override
				public boolean hasNext() {
					return index < size();
				}

				@Override
				public ColumnEntry next() {
					if(!hasNext())
						throw new NoSuchElementException();
					
					return new ColumnEntry(index++);
				}				
			};
		}

		@Override
		public int size() {
			return keyColumn.size();
		}
	}
	
	@Override
	public Set<Entry<K, V>> entrySet() {
		return new ColumnEntrySet();
	}
}
