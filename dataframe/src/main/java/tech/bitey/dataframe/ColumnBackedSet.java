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

import static java.util.Spliterator.SORTED;

import java.util.AbstractSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.Spliterator;
import java.util.Spliterators;

class ColumnBackedSet<E extends Comparable<? super E>> extends AbstractSet<E> implements NavigableSet<E> {

	private final Column<E> delegate;

	ColumnBackedSet(Column<E> delegate) {
		this.delegate = delegate;
	}

	@Override
	public Comparator<? super E> comparator() {
		return null;
	}

	@Override
	public int size() {
		return delegate.size();
	}

	@Override
	public boolean contains(Object o) {
		return delegate.contains(o);
	}

	@Override
	public Object[] toArray() {
		return delegate.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return delegate.toArray(a);
	}

	@Override
	public E first() {
		return delegate.first();
	}

	@Override
	public E last() {
		return delegate.last();
	}

	@Override
	public E lower(E e) {
		return delegate.lower(e);
	}

	@Override
	public E floor(E e) {
		return delegate.floor(e);
	}

	@Override
	public E ceiling(E e) {
		return delegate.ceiling(e);
	}

	@Override
	public E higher(E e) {
		return delegate.higher(e);
	}

	@Override
	public Iterator<E> iterator() {
		return delegate.iterator();
	}

	@Override
	public Spliterator<E> spliterator() {
		return Spliterators.spliterator(this, delegate.characteristics());
	}

	@Override
	public NavigableSet<E> descendingSet() {
		return new DescColumnBackedSet<>(this);
	}

	@Override
	public Iterator<E> descendingIterator() {
		return descendingIterator(delegate);
	}

	static <E> Iterator<E> descendingIterator(List<E> column) {
		return new Iterator<E>() {

			private final ListIterator<E> iterator = column.listIterator(column.size());

			@Override
			public boolean hasNext() {
				return iterator.hasPrevious();
			}

			@Override
			public E next() {
				return iterator.previous();
			}
		};
	}

	@Override
	public ColumnBackedSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
		return new ColumnBackedSet<>(delegate.subColumnByValue(fromElement, fromInclusive, toElement, toInclusive));
	}

	@Override
	public ColumnBackedSet<E> headSet(E toElement, boolean inclusive) {
		return new ColumnBackedSet<>(delegate.head(toElement, inclusive));
	}

	@Override
	public ColumnBackedSet<E> tailSet(E fromElement, boolean inclusive) {
		return new ColumnBackedSet<>(delegate.tail(fromElement, inclusive));
	}

	@Override
	public ColumnBackedSet<E> subSet(E fromElement, E toElement) {
		return new ColumnBackedSet<>(delegate.subColumnByValue(fromElement, toElement));
	}

	@Override
	public ColumnBackedSet<E> headSet(E toElement) {
		return new ColumnBackedSet<>(delegate.head(toElement));
	}

	@Override
	public ColumnBackedSet<E> tailSet(E fromElement) {
		return new ColumnBackedSet<>(delegate.tail(fromElement));
	}

	@Override
	public E pollFirst() {
		throw new UnsupportedOperationException("pollFirst");
	}

	@Override
	public E pollLast() {
		throw new UnsupportedOperationException("pollLast");
	}

	private static class DescColumnBackedSet<E extends Comparable<? super E>> extends AbstractSet<E>
			implements NavigableSet<E> {

		private final ColumnBackedSet<E> asc;

		DescColumnBackedSet(ColumnBackedSet<E> asc) {
			this.asc = asc;
		}

		@Override
		public Comparator<? super E> comparator() {
			return Collections.reverseOrder(asc.comparator());
		}

		@Override
		public E first() {
			return asc.last();
		}

		@Override
		public E last() {
			return asc.first();
		}

		@Override
		public int size() {
			return asc.size();
		}

		@Override
		public boolean contains(Object o) {
			return asc.contains(o);
		}

		@Override
		public E lower(E e) {
			return asc.higher(e);
		}

		@Override
		public E floor(E e) {
			return asc.ceiling(e);
		}

		@Override
		public E ceiling(E e) {
			return asc.floor(e);
		}

		@Override
		public E higher(E e) {
			return asc.lower(e);
		}

		@Override
		public E pollFirst() {
			return asc.pollLast();
		}

		@Override
		public E pollLast() {
			return asc.pollFirst();
		}

		@Override
		public Iterator<E> iterator() {
			return asc.descendingIterator();
		}

		@Override
		public Spliterator<E> spliterator() {
			return Spliterators.spliterator(this, asc.delegate.characteristics() & ~SORTED);
		}

		@Override
		public NavigableSet<E> descendingSet() {
			return asc;
		}

		@Override
		public Iterator<E> descendingIterator() {
			return asc.iterator();
		}

		@Override
		public NavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
			return new DescColumnBackedSet<>(asc.subSet(toElement, toInclusive, fromElement, fromInclusive));
		}

		@Override
		public NavigableSet<E> headSet(E toElement, boolean inclusive) {
			return new DescColumnBackedSet<>(asc.tailSet(toElement, inclusive));
		}

		@Override
		public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {
			return new DescColumnBackedSet<>(asc.headSet(fromElement, inclusive));
		}

		@Override
		public SortedSet<E> subSet(E fromElement, E toElement) {
			return new DescColumnBackedSet<>(asc.subSet(toElement, false, fromElement, true));
		}

		@Override
		public SortedSet<E> headSet(E toElement) {
			return new DescColumnBackedSet<>(asc.tailSet(toElement, false));
		}

		@Override
		public SortedSet<E> tailSet(E fromElement) {
			return new DescColumnBackedSet<>(asc.headSet(fromElement, true));
		}

	}
}
