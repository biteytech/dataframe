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

import java.util.NoSuchElementException;

/**
 * A cursor for the rows of a {@link DataFrame} that allows the programmer to
 * traverse the rows in either direction.
 * <p>
 * The difference between a cursor and an iterator is that an iterator returns a
 * {@link Row} on a call to {@code next()} or {@code previous()}, while a cursor
 * <em>is</em> a row.
 *
 * @author biteytech@protonmail.com
 */
public interface Cursor extends Row {

	/**
	 * Returns {@code true} if this dataframe cursor has more rows to traverse in
	 * the forward direction. (In other words, returns {@code true} if {@link #next}
	 * would move to a valid row rather than throwing an exception.)
	 *
	 * @return {@code true} if the cursor has more rows when traversing the
	 *         dataframe in the forward direction
	 */
	boolean hasNext();

	/**
	 * Moves the cursor to the next {@link Row} in the dataframe. This method may be
	 * called repeatedly to iterate through the dataframe, or intermixed with calls
	 * to {@link #previous} to go back and forth. (Note that alternating calls to
	 * {@code next} and {@code previous} will land on the same row repeatedly.)
	 *
	 * @throws NoSuchElementException if the dataframe has no next row
	 */
	void next();

	/**
	 * Returns {@code true} if this dataframe cursor has more rows to traverse in
	 * the backward direction. (In other words, returns {@code true} if
	 * {@link #previous} would move to a valid row rather than throwing an
	 * exception.)
	 *
	 * @return {@code true} if the cursor has more rows when traversing the
	 *         dataframe in the backward direction
	 */
	boolean hasPrevious();

	/**
	 * Returns the previous element in the dataframe and moves the cursor position
	 * backwards. This method may be called repeatedly to iterate through the
	 * dataframe backwards, or intermixed with calls to {@link #next} to go back and
	 * forth. (Note that alternating calls to {@code next} and {@code previous} will
	 * return the same element repeatedly.)
	 *
	 * @throws NoSuchElementException if the dataframe has no previous row
	 */
	void previous();
}
