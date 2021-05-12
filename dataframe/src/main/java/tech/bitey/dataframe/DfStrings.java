/*
 * Copyright (C) 2010 The Guava Authors
 * 
 *   THIS FILE HAS BEEN MODIFIED FROM ITS ORIGINAL FORM
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tech.bitey.dataframe;

/*
 * NOTE: THIS FILE HAS BEEN MODIFIED FROM ITS ORIGINAL FORM
 */

enum DfStrings {
	;

	/**
	 * Returns a string consisting of a specific number of concatenated copies of an
	 * input string. For example, {@code repeat("hey", 3)} returns the string {@code
	 * "heyheyhey"}.
	 *
	 * @param string any non-null string
	 * 
	 * @param count  the number of times to repeat it; a nonnegative integer
	 * 
	 * @return a string containing {@code string} repeated {@code count} times (the
	 *         empty string if {@code count} is zero)
	 * 
	 * @throws IllegalArgumentException if {@code count} is negative
	 */
	static String repeat(String string, int count) {
		Pr.checkNotNull(string, "string cannot be null");

		if (count <= 1) {
			Pr.checkArgument(count >= 0, "count must be nonnegative");
			return (count == 0) ? "" : string;
		}

		final int len = string.length();
		final long longSize = (long) len * (long) count;
		final int size = (int) longSize;
		if (size != longSize) {
			throw new ArrayIndexOutOfBoundsException("Required array size too large: " + longSize);
		}

		final char[] array = new char[size];
		string.getChars(0, len, array, 0);
		int n;
		for (n = len; n < size - n; n <<= 1) {
			System.arraycopy(array, 0, array, n, n);
		}
		System.arraycopy(array, 0, array, n, size - n);
		return new String(array);
	}
}
