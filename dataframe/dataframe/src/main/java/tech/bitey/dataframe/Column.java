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

import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterator.SIZED;
import static java.util.Spliterator.SORTED;
import static java.util.Spliterator.SUBSIZED;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

/**
 * An immutable {@link java.util.List List} backed by nio buffers. Elements of
 * type {@code E} are packed/unpacked to and from the buffers. There are four
 * variants for each element type, with different tradeoffs between performance
 * and functionality:
 * <table border=1>
 * <tr>
 * <th>Database<br>
 * Terminology</th>
 * <th>isNonnull</th>
 * <th>isSorted</th>
 * <th>isDistinct</th>
 * <th>{@link Spliterator}<br>
 * characteristics</th>
 * <th>Find by Value /<br>
 * Binary Search</th>
 * </tr>
 * 
 * <tr>
 * <td>Heap, NULL</td>
 * <td>FALSE</td>
 * <td>FALSE</td>
 * <td>FALSE</td>
 * <td></td>
 * <td>O(n) / No</td>
 * </tr>
 * 
 * <tr>
 * <td>Heap, NOT NULL</td>
 * <td>TRUE</td>
 * <td>FALSE</td>
 * <td>FALSE</td>
 * <td>{@link Spliterator#NONNULL NONNULL}</td>
 * <td>O(n) / No</td>
 * </tr>
 * 
 * <tr>
 * <td>Index</td>
 * <td>TRUE</td>
 * <td>TRUE</td>
 * <td>FALSE</td>
 * <td>{@link Spliterator#NONNULL NONNULL}, {@link Spliterator#SORTED
 * SORTED}</td>
 * <td>O(log(n)) / Yes</td>
 * </tr>
 * 
 * <tr>
 * <td>Unique Index</td>
 * <td>TRUE</td>
 * <td>TRUE</td>
 * <td>TRUE</td>
 * <td>{@link Spliterator#NONNULL NONNULL}, {@link Spliterator#SORTED SORTED},
 * {@link Spliterator#DISTINCT DISTINCT}</td>
 * <td>O(log(n)) / Yes</td>
 * </tr>
 * </table>
 * <p>
 * <u>Additional Notes</u>
 * <ul>
 * <li>The characteristics cannot be mixed and matched arbitrarily. Rather:
 * {@code DISTINCT} implies {@code SORTED} implies {@code NONNULL}.
 * <li>All columns report {@link Spliterator#SIZED SIZED},
 * {@link Spliterator#SUBSIZED SUBSIZED}, {@link Spliterator#ORDERED ORDERED},
 * and {@link Spliterator#IMMUTABLE IMMUTABLE} in addition to the
 * characteristics listed above.
 * </ul>
 * 
 * @author biteytech@protonmail.com
 *
 * @param <E> the type of elements in this list
 */
public interface Column<E extends Comparable<? super E>> extends List<E> {

	static int BASE_CHARACTERISTICS = SIZED | SUBSIZED | IMMUTABLE | ORDERED;

	/**
	 * Returns the {@link Spliterator#characteristics()} for this column.
	 * 
	 * @return the characteristics for this column
	 */
	int characteristics();

	/**
	 * Returns true if the {@link Spliterator#NONNULL} flag is set.
	 * 
	 * @return true if the {@code NONNULL} flag is set.
	 */
	default boolean isNonnull() {
		return (characteristics() & NONNULL) != 0;
	}

	/**
	 * Returns true if the {@link Spliterator#SORTED} flag is set.
	 * 
	 * @return true if the {@code SORTED} flag is set.
	 */
	default boolean isSorted() {
		return (characteristics() & SORTED) != 0;
	}

	/**
	 * Returns true if the {@link Spliterator#DISTINCT} flag is set.
	 * 
	 * @return true if the {@code DISTINCT} flag is set.
	 */
	default boolean isDistinct() {
		return (characteristics() & DISTINCT) != 0;
	}

	/**
	 * Converts an index into a heap.
	 * 
	 * @return a column equal to this one, but which reports {@link #isSorted} as
	 *         false. The resulting column shares the same underlying buffer as this
	 *         one.
	 */
	Column<E> toHeap();

	/**
	 * Converts a heap to an index. The resulting column will have the same
	 * elements, but sorted in ascending order. The behavior of this method depends
	 * on the {@link #characteristics()} of this column:
	 * <ul>
	 * <li>heap - checks if the column is already sorted. If so, returns a new
	 * column which shares the same underlying buffer, but with {@code SORTED} flag
	 * set. If not already sorted then {@link #copy()} will be invoked, and the
	 * resulting column will be sorted.
	 * <li>sorted - returns this column
	 * <li>distinct - returns a new column which shares the same underlying buffer,
	 * but with {@code DISTINCT} flag unset.
	 * </ul>
	 * 
	 * @return a column with elements sorted in ascending order
	 * 
	 * @throws UnsupportedOperationException if the {@code NONNULL} flag is not set
	 */
	Column<E> toSorted();

	/**
	 * Converts a column to a unique index. The behavior of this method depends on
	 * the {@link #characteristics()} of this column:
	 * <ul>
	 * <li>heap - checks if the column is already sorted and distinct. If so,
	 * returns a new column which shares the same underlying buffer, but with
	 * {@code SORTED} and {@code DISTINCT} flags set. If not already sorted and
	 * distinct then {@link #copy()} will be invoked, and the resulting column will
	 * be sorted and deduplicated.
	 * <li>sorted - checks if the column contains duplicates. If not, returns a new
	 * column which shares the same underlying buffer, but with {@code DISTINCT}
	 * flag set. If duplicates are present then {@link #copy()} will be invoked, and
	 * the resulting column will be deduplicated.
	 * <li>distinct - returns this column
	 * </ul>
	 * 
	 * @return a column with distinct elements sorted in ascending order
	 * 
	 * @throws UnsupportedOperationException if the {@code NONNULL} flag is not set
	 */
	Column<E> toDistinct();

	/**
	 * @return this column's {@link ColumnType type}.
	 */
	ColumnType<E> getType();

	/**
	 * Test if a value is null at a given index.
	 * 
	 * @param index the index to test
	 * 
	 * @return true iff the value at the given index is null
	 */
	boolean isNull(int index);

	/**
	 * Returns a view of the portion of this column between the specified
	 * {@code fromIndex}, inclusive, and {@code toIndex}, exclusive. (If
	 * {@code fromIndex} and {@code toIndex} are equal, the returned list is empty.)
	 * The returned column is backed by this column.
	 * 
	 * @param fromIndex low endpoint (inclusive) of the subList
	 * @param toIndex   high endpoint (exclusive) of the subList
	 * @return a view of the specified range within this column
	 * @throws IndexOutOfBoundsException for an illegal endpoint index value
	 *                                   ({@code fromIndex < 0 || toIndex > size ||
	 *         fromIndex > toIndex}   )
	 */
	Column<E> subColumn(int fromIndex, int toIndex);

	/**
	 * Creates a {@link Spliterator} over the elements in this column.
	 *
	 * @return a {@code Spliterator} over the elements in this column.
	 */
	@Override
	default Spliterator<E> spliterator() {
		return Spliterators.spliterator(this, characteristics());
	}

	/**
	 * Appends two columns with the same element type.
	 * <p>
	 * Both columns must have the same characteristics. If they're both unique
	 * indices then the first value of the provided column must be greater than the
	 * last value of this column.
	 * 
	 * @param tail - the column to be appended to the end of this column
	 * 
	 * @return the provided column appended to the end of this column
	 */
	Column<E> append(Column<E> tail);

	/**
	 * Appends two columns with the same element type.
	 * <p>
	 * If coerce is true and one of the columns is sorted, the sorted column will be
	 * converted to a heap before appending. Otherwise this method behaves like
	 * {@link #append(Column)}
	 * 
	 * @param tail   - the column to be appended to the end of this column
	 * @param coerce - specifies if a sole sorted column should be converted to a
	 *               heap
	 * 
	 * @return the provided column appended to the end of this column
	 */
	default Column<E> append(Column<E> tail, boolean coerce) {
		if (coerce) {
			if (isSorted() && !tail.isSorted())
				return toHeap().append(tail);
			else if (!isSorted() && tail.isSorted())
				return append(tail.toHeap());
		}

		return append(tail);
	}

	/**
	 * Returns a column equal to this one, but with elements stored in a newly
	 * allocated buffer.
	 * 
	 * @return a column equal to this one, but with elements stored in a newly
	 *         allocated buffer.
	 */
	Column<E> copy();

	/**
	 * Returns a {@link NavigableSet} view of this column.
	 * <p>
	 * Column must be a unique index.
	 * 
	 * @return a {@code NavigableSet} view of this column.
	 * 
	 * @throws UnsupportedOperationException if {@link #isDistinct()} return false
	 */
	NavigableSet<E> asSet();

	/*------------------------------------------------------------
	 *  Type Transformation Methods
	 *------------------------------------------------------------*/
	/**
	 * Convert this column into a {@link BooleanColumn} by applying the specified
	 * {@link Predicate} to each non-null element. Nulls are preserved as-is.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link BooleanColumn} created from this column using the specified
	 *         {@code Predicate}.
	 */
	public BooleanColumn toBooleanColumn(Predicate<E> predicate);

	/**
	 * Convert this column into a {@link DateColumn} by applying the specified
	 * {@link Function} to each non-null element. Nulls are preserved as-is, but
	 * <u>the function must not return null</u>.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link DateColumn} created from this column using the specified
	 *         {@code Function}.
	 */
	public DateColumn toDateColumn(Function<E, LocalDate> function);

	/**
	 * Convert this column into a {@link DateTimeColumn} by applying the specified
	 * {@link Function} to each non-null element. Nulls are preserved as-is, but
	 * <u>the function must not return null</u>.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link DateTimeColumn} created from this column using the specified
	 *         {@code Function}.
	 */
	public DateTimeColumn toDateTimeColumn(Function<E, LocalDateTime> function);

	/**
	 * Convert this column into a {@link DoubleColumn} by applying the specified
	 * {@link ToDoubleFunction} to each non-null element.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link DoubleColumn} created from this column using the specified
	 *         {@code ToDoubleFunction}.
	 */
	public DoubleColumn toDoubleColumn(ToDoubleFunction<E> function);

	/**
	 * Convert this column into a {@link FloatColumn} by applying the specified
	 * {@link ToFloatFunction} to each non-null element.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link FloatColumn} created from this column using the specified
	 *         {@code ToFloatFunction}.
	 */
	public FloatColumn toFloatColumn(ToFloatFunction<E> function);

	/**
	 * Convert this column into a {@link IntColumn} by applying the specified
	 * {@link ToIntFunction} to each non-null element.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return An {@link IntColumn} created from this column using the specified
	 *         {@code ToIntFunction}.
	 */
	public IntColumn toIntColumn(ToIntFunction<E> function);

	/**
	 * Convert this column into a {@link LongColumn} by applying the specified
	 * {@link ToLongFunction} to each non-null element.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link LongColumn} created from this column using the specified
	 *         {@code ToLongFunction}.
	 */
	public LongColumn toLongColumn(ToLongFunction<E> function);

	/**
	 * Convert this column into a {@link ShortColumn} by applying the specified
	 * {@link ToShortFunction} to each non-null element.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link ShortColumn} created from this column using the specified
	 *         {@code ToShortFunction}.
	 */
	public ShortColumn toShortColumn(ToShortFunction<E> function);

	/**
	 * Convert this column into a {@link ByteColumn} by applying the specified
	 * {@link ToByteFunction} to each non-null element.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link ByteColumn} created from this column using the specified
	 *         {@code ToByteFunction}.
	 */
	public ByteColumn toByteColumn(ToByteFunction<E> function);

	/**
	 * Convert this column into a {@link DecimalColumn} by applying the specified
	 * {@link Function} to each non-null element. Nulls are preserved as-is, but
	 * <u>the function most not return null</u>.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link DecimalColumn} created from this column using the specified
	 *         {@code Function}.
	 */
	public DecimalColumn toDecimalColumn(Function<E, BigDecimal> function);

	/**
	 * Convert this column into a {@link StringColumn} by applying the specified
	 * {@link Function} to each non-null element. Nulls are preserved as-is, but
	 * <u>the function most not return null</u>.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link StringColumn} created from this column using the specified
	 *         {@code Function}.
	 */
	public StringColumn toStringColumn(Function<E, String> function);

	/**
	 * Convert this column into a {@link StringColumn} by applying
	 * {@link Object#toString} to each non-null element. Nulls are preserved as-is,
	 * but <u>the function most not return null</u>.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link StringColumn} created from this column using
	 *         {@link Object#toString}.
	 */
	default StringColumn toStringColumn() {
		return toStringColumn(Object::toString);
	}

	/**
	 * Convert this column into a {@link UuidColumn} by applying the specified
	 * {@link Function} to each non-null element. Nulls are preserved as-is, but
	 * <u>the function most not return null</u>.
	 * <p>
	 * The resulting column will not be flagged as sorted or distinct.
	 * 
	 * @return A {@link UuidColumn} created from this column using the specified
	 *         {@code Function}.
	 */
	public UuidColumn toUuidColumn(Function<E, UUID> function);

	/*------------------------------------------------------------
	 *  Other Transformation Methods
	 *------------------------------------------------------------*/
	/**
	 * Returns a new column derived by testing each value with the specified
	 * predicate and replacing with {@code null} when predicate returns
	 * {@code true}.
	 * 
	 * @param predicate the {@link Predicate} used to test for values which should
	 *                  be {@code null}
	 * 
	 * @return a new column derived by testing each value with the specified
	 *         predicate and replacing with {@code null} when predicate returns
	 *         {@code true}.
	 */
	Column<E> clean(Predicate<E> predicate);

	/**
	 * Returns a new column derived by testing each value with the specified
	 * predicate and removing values when the predicate returns {@code false}.
	 * {@code null} values are never passed to the predicate for testing, instead
	 * the {@code keepNulls} parameter determines whether all nulls are kept as-is,
	 * or if all nulls are removed.
	 * 
	 * @param predicate the {@link Predicate} used to test for values which should
	 *                  be kept.
	 * @param keepNulls {@code true} means keep all {@code null} values as-is.
	 *                  {@code false} means drop all {@code null} values.
	 * 
	 * @return a new column derived by testing each value with the specified
	 *         predicate.
	 */
	Column<E> filter(Predicate<E> predicate, boolean keepNulls);

	/*------------------------------------------------------------
	 *  NavigableSet-inspired Methods
	 *------------------------------------------------------------*/
	/**
	 * Returns the first (lowest) element in this column.
	 *
	 * @return the first (lowest) element in this column
	 * 
	 * @throws NoSuchElementException if this column is empty
	 */
	E first();

	/**
	 * Returns the last (highest) element in this column.
	 *
	 * @return the last (highest) element in this column
	 * 
	 * @throws NoSuchElementException if this column is empty
	 */
	E last();

	/**
	 * Returns the greatest element in this column strictly less than the given
	 * element, or {@code null} if there is no such element.
	 *
	 * @param value the value to match
	 * 
	 * @return the greatest element less than {@code e}, or {@code null} if there is
	 *         no such element
	 * 
	 * @throws UnsupportedOperationException if column is not a unique index, i.e.
	 *                                       the DISTINCT flag is not set
	 */
	E lower(E value);

	/**
	 * Returns the least element in this column strictly greater than the given
	 * element, or {@code null} if there is no such element.
	 *
	 * @param value the value to match
	 * 
	 * @return the least element greater than {@code e}, or {@code null} if there is
	 *         no such element
	 * 
	 * @throws UnsupportedOperationException if column is not a unique index, i.e.
	 *                                       the DISTINCT flag is not set
	 */
	E higher(E value);

	/**
	 * Returns the greatest element in this column less than or equal to the given
	 * element, or {@code null} if there is no such element.
	 *
	 * @param value the value to match
	 * 
	 * @return the greatest element less than or equal to {@code e}, or {@code null}
	 *         if there is no such element
	 * 
	 * @throws UnsupportedOperationException if column is not a unique index, i.e.
	 *                                       the DISTINCT flag is not set
	 */
	E floor(E value);

	/**
	 * Returns the least element in this column greater than or equal to the given
	 * element, or {@code null} if there is no such element.
	 *
	 * @param value the value to match
	 * 
	 * @return the least element greater than or equal to {@code e}, or {@code null}
	 *         if there is no such element
	 * 
	 * @throws UnsupportedOperationException if column is not a unique index, i.e.
	 *                                       the DISTINCT flag is not set
	 */
	E ceiling(E value);

	/**
	 * Returns a view of the portion of this column whose elements range from
	 * {@code fromElement} to {@code toElement}. If {@code fromElement} and
	 * {@code toElement} are equal, the returned column is empty unless {@code
	 * fromInclusive} and {@code toInclusive} are both true. The returned column is
	 * backed by this column.
	 * <p>
	 * <em>This method is only available when {@link #isDistinct()} returns
	 * true.</em>
	 *
	 * @param fromElement   low endpoint of the returned column
	 * @param fromInclusive true if the low endpoint is to be included in the result
	 * @param toElement     high endpoint of the returned column
	 * @param toInclusive   true if the high endpoint is to be included in the
	 *                      result
	 * @return a view of the portion of this column whose elements range from
	 *         {@code fromElement} to {@code toElement}
	 * @throws UnsupportedOperationException if {@link #isDistinct()} return false
	 * @throws IllegalArgumentException      if {@code fromElement} is greater than
	 *                                       {@code toElement}
	 */
	Column<E> subColumnByValue(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive);

	/**
	 * Same behavior as {@code Column#subColumn(Object, boolean, Object, Boolean)},
	 * with {@code fromInclusive} set to true and {@code toInclusive} set to false.
	 * <p>
	 * <em>This method is only available when {@link #isDistinct()} returns
	 * true.</em>
	 * 
	 * @param fromElement low endpoint of the returned column, inclusive
	 * @param toElement   high endpoint of the returned column, exclusive
	 * 
	 * @return view of the portion of this column whose elements range from
	 *         {@code fromElement}, inclusive, to {@code toElement}, exclusive
	 */
	Column<E> subColumnByValue(E fromElement, E toElement);

	/**
	 * Returns a view of the portion of this column whose elements are less than (or
	 * equal to, if {@code inclusive} is true) {@code toElement}. The returned
	 * column is backed by this column.
	 * <p>
	 * <em>This method is only available when {@link #isDistinct()} returns
	 * true.</em>
	 *
	 * @param toElement high endpoint of the returned column
	 * @param inclusive {@code true} if the high endpoint is to be included in the
	 *                  returned view
	 * @return a view of the portion of this column whose elements are less than (or
	 *         equal to, if {@code inclusive} is true) {@code toElement}
	 * @throws UnsupportedOperationException if {@link #isDistinct()} return false
	 */
	Column<E> head(E toElement, boolean inclusive);

	/**
	 * Same behavior as {@link #head(Object, boolean)}, with {@code inclusive} set
	 * to false.
	 * <p>
	 * <em>This method is only available when {@link #isDistinct()} returns
	 * true.</em>
	 * 
	 * @param toElement high endpoint of the returned column
	 * 
	 * @return a view of the portion of this column whose elements are less than
	 *         {@code toElement}
	 */
	Column<E> head(E toElement);

	/**
	 * Returns a view of the portion of this column whose elements are greater than
	 * (or equal to, if {@code inclusive} is true) {@code fromElement}. The returned
	 * column is backed by this column.
	 * <p>
	 * <em>This method is only available when {@link #isDistinct()} returns
	 * true.</em>
	 *
	 * @param fromElement low endpoint of the returned column
	 * @param inclusive   {@code true} if the low endpoint is to be included in the
	 *                    returned view
	 * @return a view of the portion of this column whose elements are greater than
	 *         or equal to {@code fromElement}
	 * @throws UnsupportedOperationException if {@link #isDistinct()} return false
	 */
	Column<E> tail(E fromElement, boolean inclusive);

	/**
	 * Same behavior as {@link #tail(Object, boolean)}, with {@code inclusive} set
	 * to true.
	 * <p>
	 * <em>This method is only available when {@link #isDistinct()} returns
	 * true.</em>
	 * 
	 * @param fromElement low endpoint of the returned column
	 * 
	 * @return a view of the portion of this column whose elements are greater than
	 *         or equal to {@code fromElement}
	 */
	Column<E> tail(E fromElement);
}
