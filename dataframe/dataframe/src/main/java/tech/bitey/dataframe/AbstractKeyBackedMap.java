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

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.Spliterator;
import java.util.Spliterators;

abstract class AbstractKeyBackedMap<K extends Comparable<? super K>, V> extends AbstractMap<K, V>
		implements NavigableMap<K, V> {

	final NonNullColumn<K, ?, ?> keyColumn;

	AbstractKeyBackedMap(Column<K> keyColumn) {

		this.keyColumn = (NonNullColumn<K, ?, ?>) keyColumn;
	}

	abstract Entry<K, V> entry(int index);

	abstract Iterator<V> descendingValuesIterator();

	abstract int valuesCharacteristics();

	@Override
	public abstract AbstractKeyBackedMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive);

	@Override
	public abstract AbstractKeyBackedMap<K, V> headMap(K toKey, boolean inclusive);

	@Override
	public abstract AbstractKeyBackedMap<K, V> tailMap(K fromKey, boolean inclusive);

	@Override
	public abstract AbstractKeyBackedMap<K, V> subMap(K fromKey, K toKey);

	@Override
	public boolean containsKey(Object o) {
		return keyColumn.contains(o);
	}

	@Override
	public int size() {
		return keyColumn.size();
	}

	@Override
	public NavigableSet<K> keySet() {
		return keyColumn.asSet();
	}

	@Override
	public NavigableSet<K> navigableKeySet() {
		return keySet();
	}

	@Override
	public NavigableSet<K> descendingKeySet() {
		return navigableKeySet().descendingSet();
	}

	@Override
	public Comparator<? super K> comparator() {
		return null;
	}

	@Override
	public K firstKey() {
		return keyColumn.first();
	}

	@Override
	public K lastKey() {
		return keyColumn.last();
	}

	@Override
	public Entry<K, V> lowerEntry(K key) {
		int index = keyColumn.lowerIndex(key);
		return index == -1 ? null : entry(index - keyColumn.offset);
	}

	@Override
	public K lowerKey(K key) {
		return keyColumn.lower(key);
	}

	@Override
	public Entry<K, V> floorEntry(K key) {
		int index = keyColumn.floorIndex(key);
		return index == -1 ? null : entry(index - keyColumn.offset);
	}

	@Override
	public K floorKey(K key) {
		return keyColumn.floor(key);
	}

	@Override
	public Entry<K, V> ceilingEntry(K key) {
		int index = keyColumn.ceilingIndex(key);
		return index == -1 ? null : entry(index - keyColumn.offset);
	}

	@Override
	public K ceilingKey(K key) {
		return keyColumn.ceiling(key);
	}

	@Override
	public Entry<K, V> higherEntry(K key) {
		int index = keyColumn.higherIndex(key);
		return index == -1 ? null : entry(index - keyColumn.offset);
	}

	@Override
	public K higherKey(K key) {
		return keyColumn.higher(key);
	}

	@Override
	public Entry<K, V> firstEntry() {
		return isEmpty() ? null : entry(0);
	}

	@Override
	public Entry<K, V> lastEntry() {
		return isEmpty() ? null : entry(size() - 1);
	}

	@Override
	public Entry<K, V> pollFirstEntry() {
		throw new UnsupportedOperationException("pollFirstEntry");
	}

	@Override
	public Entry<K, V> pollLastEntry() {
		throw new UnsupportedOperationException("pollLastEntry");
	}

	@Override
	public NavigableMap<K, V> descendingMap() {
		return new DescAbstractKeyBackedMap<>(this);
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		return new KeyBackendEntrySet();
	}

	private class KeyBackendEntrySet extends AbstractSet<Entry<K, V>> {
		@Override
		public Iterator<Entry<K, V>> iterator() {
			return new Iterator<Entry<K, V>>() {

				int index = 0;

				@Override
				public boolean hasNext() {
					return index < size();
				}

				@Override
				public Entry<K, V> next() {
					if (!hasNext())
						throw new NoSuchElementException();

					return entry(index++);
				}
			};
		}

		@Override
		public int size() {
			return keyColumn.size();
		}

		@Override
		public boolean removeAll(Collection<?> c) {
			throw new UnsupportedOperationException("removeAll");
		}
	}

	static abstract class AbstractEntry<K, V> implements Map.Entry<K, V> {

		final int index;

		AbstractEntry(int index) {
			this.index = index;
		}

		@Override
		public V setValue(V value) {
			throw new UnsupportedOperationException("setValue");
		}

		@Override
		public String toString() {
			return getKey() + "=" + getValue();
		}

		@Override
		public int hashCode() {
			return Objects.hash(getKey(), getValue());
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof Map.Entry))
				return false;

			@SuppressWarnings("rawtypes")
			Map.Entry e = (Map.Entry) o;

			return getKey().equals(e.getKey()) && Objects.equals(getValue(), e.getValue());
		}
	}

	private static class DescAbstractKeyBackedMap<K extends Comparable<? super K>, V> extends AbstractMap<K, V>
			implements NavigableMap<K, V> {

		private final AbstractKeyBackedMap<K, V> asc;

		DescAbstractKeyBackedMap(AbstractKeyBackedMap<K, V> asc) {
			this.asc = asc;
		}

		@Override
		public Comparator<? super K> comparator() {
			return Collections.reverseOrder(asc.comparator());
		}

		@Override
		public K firstKey() {
			return asc.lastKey();
		}

		@Override
		public K lastKey() {
			return asc.firstKey();
		}

		@Override
		public Set<K> keySet() {
			return asc.descendingKeySet();
		}

		@Override
		public Collection<V> values() {
			return new AbstractCollection<V>() {
				@Override
				public Iterator<V> iterator() {
					return asc.descendingValuesIterator();
				}

				@Override
				public int size() {
					return asc.size();
				}

				@Override
				public Spliterator<V> spliterator() {
					return Spliterators.spliterator(this, asc.valuesCharacteristics() & ~SORTED);
				}
			};
		}

		@Override
		public Set<Entry<K, V>> entrySet() {
			return new AbstractSet<Entry<K, V>>() {
				@Override
				public Iterator<Entry<K, V>> iterator() {
					return new Iterator<Entry<K, V>>() {

						int index = asc.size() - 1;

						@Override
						public boolean hasNext() {
							return index >= 0;
						}

						@Override
						public Entry<K, V> next() {
							if (!hasNext())
								throw new NoSuchElementException();

							return asc.entry(index--);
						}
					};
				}

				@Override
				public int size() {
					return asc.size();
				}

				@Override
				public Spliterator<Entry<K, V>> spliterator() {
					return Spliterators.spliterator(this, NonNullColumn.NONNULL_CHARACTERISTICS | DISTINCT);
				}

				@Override
				public boolean removeAll(Collection<?> c) {
					throw new UnsupportedOperationException("removeAll");
				}
			};
		}

		@Override
		public int size() {
			return asc.size();
		}

		@Override
		public boolean containsKey(Object key) {
			return asc.containsKey(key);
		}

		@Override
		public boolean containsValue(Object value) {
			return asc.containsValue(value);
		}

		@Override
		public V get(Object key) {
			return asc.get(key);
		}

		@Override
		public Entry<K, V> lowerEntry(K key) {
			return asc.higherEntry(key);
		}

		@Override
		public K lowerKey(K key) {
			return asc.higherKey(key);
		}

		@Override
		public Entry<K, V> floorEntry(K key) {
			return asc.ceilingEntry(key);
		}

		@Override
		public K floorKey(K key) {
			return asc.ceilingKey(key);
		}

		@Override
		public Entry<K, V> ceilingEntry(K key) {
			return asc.floorEntry(key);
		}

		@Override
		public K ceilingKey(K key) {
			return asc.floorKey(key);
		}

		@Override
		public Entry<K, V> higherEntry(K key) {
			return asc.lowerEntry(key);
		}

		@Override
		public K higherKey(K key) {
			return asc.lowerKey(key);
		}

		@Override
		public Entry<K, V> firstEntry() {
			return asc.lastEntry();
		}

		@Override
		public Entry<K, V> lastEntry() {
			return asc.firstEntry();
		}

		@Override
		public Entry<K, V> pollFirstEntry() {
			return asc.pollLastEntry();
		}

		@Override
		public Entry<K, V> pollLastEntry() {
			return asc.pollFirstEntry();
		}

		@Override
		public NavigableMap<K, V> descendingMap() {
			return asc;
		}

		@Override
		public NavigableSet<K> navigableKeySet() {
			return asc.descendingKeySet();
		}

		@Override
		public NavigableSet<K> descendingKeySet() {
			return asc.navigableKeySet();
		}

		@Override
		public NavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
			return new DescAbstractKeyBackedMap<>(asc.subMap(toKey, toInclusive, fromKey, fromInclusive));
		}

		@Override
		public NavigableMap<K, V> headMap(K toKey, boolean inclusive) {
			return new DescAbstractKeyBackedMap<>(asc.tailMap(toKey, inclusive));
		}

		@Override
		public NavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
			return new DescAbstractKeyBackedMap<>(asc.headMap(fromKey, inclusive));
		}

		@Override
		public SortedMap<K, V> subMap(K fromKey, K toKey) {
			return new DescAbstractKeyBackedMap<>(asc.subMap(toKey, false, fromKey, true));
		}

		@Override
		public SortedMap<K, V> headMap(K toKey) {
			return new DescAbstractKeyBackedMap<>(asc.tailMap(toKey, false));
		}

		@Override
		public SortedMap<K, V> tailMap(K toKey) {
			return new DescAbstractKeyBackedMap<>(asc.headMap(toKey, true));
		}

	}
}
