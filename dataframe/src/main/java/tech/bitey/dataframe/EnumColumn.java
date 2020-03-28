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

import static tech.bitey.dataframe.DfPreconditions.checkNotNull;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An {@code EnumColumn} is a view wrapping a {@link ByteColumn}, with the
 * {@code byte} values interpreted as ordinals of the specified enum type.
 * <p>
 * {@code EnumColumns} are not first-class columns - they cannot be directly
 * added to a {@link DataFrame}, and the {@link #getType()} method returns null.
 * 
 * @author biteytech@protonmail.com
 *
 * @param <T> - enum element type
 * 
 * @see #wrap(Class, ByteColumn)
 * @see #unwrap()
 */
public class EnumColumn<T extends Enum<T>> implements Column<T> {

	private static final Map<Class<?>, Enum<?>[]> VALUES_MAP = new ConcurrentHashMap<>();

	private final Class<T> enumType;

	private final ByteColumn delegate;

	private final T[] values;

	@SuppressWarnings("unchecked")
	private EnumColumn(Class<T> enumType, ByteColumn delegate) {

		this.enumType = checkNotNull(enumType, "enumType cannot be null");
		this.delegate = checkNotNull(delegate, "delegate cannot be null");

		values = (T[]) VALUES_MAP.computeIfAbsent(enumType, x -> enumType.getEnumConstants());
	}

	/**
	 * Wraps the specified {@link ByteColumn}, interpreting the {@code byte} values
	 * as ordinals of the specified enum type.
	 * 
	 * @param enumType - the enum element type
	 * @param delegate - the {@code ByteColumn} containing ordinal values
	 * 
	 * @param <T>      - enum element type
	 * 
	 * @return an enum view over the specified {@code ByteColumn}
	 */
	public static <T extends Enum<T>> EnumColumn<T> wrap(Class<T> enumType, ByteColumn delegate) {
		return new EnumColumn<>(enumType, delegate);
	}

	/**
	 * Returns the {@link ByteColumn} containing the ordinal values.
	 * 
	 * @return the {@code ByteColumn} containing the ordinal values.
	 */
	public ByteColumn unwrap() {
		return delegate;
	}

	@Override
	public boolean equals(Object o) {

		if (o instanceof EnumColumn) {
			EnumColumn<?> rhs = (EnumColumn<?>) o;

			return enumType == rhs.enumType && delegate.equals(rhs.delegate);
		} else if (o instanceof List) {
			ListIterator<T> e1 = listIterator();
			ListIterator<?> e2 = ((List<?>) o).listIterator();
			while (e1.hasNext() && e2.hasNext()) {
				T o1 = e1.next();
				Object o2 = e2.next();
				if (!(o1 == null ? o2 == null : o1.equals(o2)))
					return false;
			}
			return !(e1.hasNext() || e2.hasNext());
		} else if (isDistinct() && o instanceof Set) {
			// from AbstractSet

			Collection<?> c = (Collection<?>) o;
			if (c.size() != size())
				return false;
			try {
				return containsAll(c);
			} catch (ClassCastException unused) {
				return false;
			} catch (NullPointerException unused) {
				return false;
			}
		} else
			return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(enumType, delegate);
	}

	@Override
	public int size() {
		return delegate.size();
	}

	@Override
	public boolean isEmpty() {
		return delegate.isEmpty();
	}

	@Override
	public boolean contains(Object o) {

		if (o == null)
			return delegate.contains(null);
		else if (o.getClass() != enumType)
			return false;

		@SuppressWarnings("unchecked")
		T t = (T) o;

		return delegate.contains((byte) t.ordinal());
	}

	@Override
	public T get(int index) {

		if (delegate.isNull(index))
			return null;

		byte ordinal = delegate.get(index);
		return values[ordinal];
	}

	@Override
	public int indexOf(Object o) {

		if (o == null)
			return delegate.indexOf(null);
		else if (o.getClass() != enumType)
			return -1;

		@SuppressWarnings("unchecked")
		T t = (T) o;

		return delegate.indexOf((byte) t.ordinal());
	}

	@Override
	public int lastIndexOf(Object o) {

		if (o == null)
			return delegate.lastIndexOf(null);
		else if (o.getClass() != enumType)
			return -1;

		@SuppressWarnings("unchecked")
		T t = (T) o;

		return delegate.lastIndexOf((byte) t.ordinal());
	}

	@Override
	public Object[] toArray() {
		Object[] a = delegate.toArray();
		for (int i = 0; i < a.length; i++)
			a[i] = values[(Byte) a[i]];
		return a;
	}

	@Override
	public <E> E[] toArray(E[] a) {
		return AbstractColumn.toArray(a, this);
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		for (Object o : c)
			if (!contains(o))
				return false;
		return true;
	}

	@Override
	public Iterator<T> iterator() {
		return listIterator();
	}

	@Override
	public ListIterator<T> listIterator() {
		return listIterator(0);
	}

	@Override
	public ListIterator<T> listIterator(int index) {
		return new ListIterator<T>() {

			private final ListIterator<Byte> delegate = EnumColumn.this.delegate.listIterator(index);

			@Override
			public boolean hasNext() {
				return delegate.hasNext();
			}

			@Override
			public T next() {
				Byte ordinal = delegate.next();
				return ordinal == null ? null : values[ordinal];
			}

			@Override
			public boolean hasPrevious() {
				return delegate.hasPrevious();
			}

			@Override
			public T previous() {
				Byte ordinal = delegate.previous();
				return ordinal == null ? null : values[ordinal];
			}

			@Override
			public int nextIndex() {
				return delegate.nextIndex();
			}

			@Override
			public int previousIndex() {
				return delegate.previousIndex();
			}

			@Override
			public void remove() {
				delegate.remove();
			}

			@Override
			public void set(T e) {
				throw new UnsupportedOperationException("set");
			}

			@Override
			public void add(T e) {
				throw new UnsupportedOperationException("add");
			}
		};
	}

	@Override
	public List<T> subList(int fromIndex, int toIndex) {
		return subColumn(fromIndex, toIndex);
	}

	@Override
	public int characteristics() {
		return delegate.characteristics();
	}

	@Override
	public EnumColumn<T> toHeap() {
		return wrap(enumType, delegate.toHeap());
	}

	@Override
	public EnumColumn<T> toSorted() {
		return wrap(enumType, delegate.toSorted());
	}

	@Override
	public EnumColumn<T> toDistinct() {
		return wrap(enumType, delegate.toDistinct());
	}

	@Override
	public ColumnType<T> getType() {
		return null;
	}

	@Override
	public boolean isNull(int index) {
		return delegate.isNull(index);
	}

	@Override
	public EnumColumn<T> subColumn(int fromIndex, int toIndex) {
		return wrap(enumType, delegate.subColumn(fromIndex, toIndex));
	}

	@Override
	public EnumColumn<T> append(Column<T> tail) {

		EnumColumn<T> column = (EnumColumn<T>) tail;

		return wrap(enumType, delegate.append(column.delegate));
	}

	@Override
	public EnumColumn<T> copy() {
		return wrap(enumType, delegate.copy());
	}

	@Override
	public Comparator<? super T> comparator() {
		return Comparator.naturalOrder();
	}

	@Override
	public T first() {
		Byte ordinal = delegate.first();
		return ordinal == null ? null : values[ordinal];
	}

	@Override
	public T last() {
		Byte ordinal = delegate.last();
		return ordinal == null ? null : values[ordinal];
	}

	@Override
	public T lower(T value) {
		Byte ordinal = delegate.lower((byte) value.ordinal());
		return ordinal == null ? null : values[ordinal];
	}

	@Override
	public T higher(T value) {
		Byte ordinal = delegate.higher((byte) value.ordinal());
		return ordinal == null ? null : values[ordinal];
	}

	@Override
	public T floor(T value) {
		Byte ordinal = delegate.floor((byte) value.ordinal());
		return ordinal == null ? null : values[ordinal];
	}

	@Override
	public T ceiling(T value) {
		Byte ordinal = delegate.ceiling((byte) value.ordinal());
		return ordinal == null ? null : values[ordinal];
	}

	@Override
	public EnumColumn<T> subColumnByValue(T fromElement, boolean fromInclusive, T toElement, boolean toInclusive) {
		return wrap(enumType, delegate.subColumnByValue((byte) fromElement.ordinal(), fromInclusive,
				(byte) toElement.ordinal(), toInclusive));
	}

	@Override
	public EnumColumn<T> subColumnByValue(T fromElement, T toElement) {
		return wrap(enumType, delegate.subColumnByValue((byte) fromElement.ordinal(), (byte) toElement.ordinal()));
	}

	@Override
	public EnumColumn<T> head(T toElement, boolean inclusive) {
		return wrap(enumType, delegate.head((byte) toElement.ordinal(), inclusive));
	}

	@Override
	public EnumColumn<T> head(T toElement) {
		return wrap(enumType, delegate.head((byte) toElement.ordinal()));
	}

	@Override
	public EnumColumn<T> tail(T fromElement, boolean inclusive) {
		return wrap(enumType, delegate.tail((byte) fromElement.ordinal(), inclusive));
	}

	@Override
	public EnumColumn<T> tail(T fromElement) {
		return wrap(enumType, delegate.tail((byte) fromElement.ordinal()));
	}

	/*------------------------------------------------------------
	 *  Mutating operations throw UnsupportedOperationException
	 *------------------------------------------------------------*/
	@Override
	public boolean add(T e) {
		throw new UnsupportedOperationException("add");
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException("remove");
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		throw new UnsupportedOperationException("addAll");
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException("removeAll");
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException("retainAll");
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException("clear");
	}

	// from list
	@Override
	public boolean addAll(int index, Collection<? extends T> c) {
		throw new UnsupportedOperationException("addAll");
	}

	@Override
	public T set(int index, T element) {
		throw new UnsupportedOperationException("set");
	}

	@Override
	public void add(int index, T element) {
		throw new UnsupportedOperationException("add");
	}

	@Override
	public T remove(int index) {
		throw new UnsupportedOperationException("remove");
	}
}
