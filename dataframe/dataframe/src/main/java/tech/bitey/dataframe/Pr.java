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

/*
 * Preconditions. Inspired by Google Guava's Preconditions.
 */
enum Pr {
	;

	static void checkArgument(boolean expression, String errorMessage) {
		if (!expression) {
			throw new IllegalArgumentException(errorMessage);
		}
	}

	static void checkState(boolean expression, String errorMessage) {
		if (!expression) {
			throw new IllegalStateException(errorMessage);
		}
	}

	static <T> T checkNotNull(T reference, String errorMessage) {
		if (reference == null) {
			throw new NullPointerException(errorMessage);
		}
		return reference;
	}

	static int checkElementIndex(int index, int size) {
		return checkElementIndex(index, size, "index");
	}

	static int checkElementIndex(int index, int size, String desc) {
		if (index < 0 || index >= size) {
			throw new IndexOutOfBoundsException(badElementIndex(index, size, desc));
		}
		return index;
	}

	private static String badElementIndex(int index, int size, String desc) {
		if (index < 0) {
			return String.format("%s (%s) must not be negative", desc, index);
		} else if (size < 0) {
			throw new IllegalArgumentException("negative size: " + size);
		} else { // index >= size
			return String.format("%s (%s) must be less than size (%s)", desc, index, size);
		}
	}

	static int checkPositionIndex(int index, int size) {
		return checkPositionIndex(index, size, "index");
	}

	static int checkPositionIndex(int index, int size, String desc) {
		if (index < 0 || index > size) {
			throw new IndexOutOfBoundsException(badPositionIndex(index, size, desc));
		}
		return index;
	}

	private static String badPositionIndex(int index, int size, String desc) {
		if (index < 0) {
			return String.format("%s (%s) must not be negative", desc, index);
		} else if (size < 0) {
			throw new IllegalArgumentException("negative size: " + size);
		} else { // index > size
			return String.format("%s (%s) must not be greater than size (%s)", desc, index, size);
		}
	}

	static void checkPositionIndexes(int start, int end, int size) {
		if (start < 0 || end < start || end > size) {
			throw new IndexOutOfBoundsException(badPositionIndexes(start, end, size));
		}
	}

	static String badPositionIndexes(int start, int end, int size) {
		if (start < 0 || start > size) {
			return badPositionIndex(start, size, "start index");
		}
		if (end < 0 || end > size) {
			return badPositionIndex(end, size, "end index");
		}
		// end < start
		return String.format("end index (%s) must not be less than start index (%s)", end, start);
	}
}
