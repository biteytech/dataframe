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

import java.util.stream.Collector;

public interface BooleanColumn extends Column<Boolean> {

	@Override
	BooleanColumn subColumn(int fromIndex, int toIndex);

	@Override
	BooleanColumn append(Column<Boolean> tail);

	@Override
	BooleanColumn copy();

	public static BooleanColumnBuilder builder() {
		return new BooleanColumnBuilder();
	}

	/**
	 * Primitive specialization of {@link Column#get(int)}.
	 * 
	 * @param index - index of the value to return
	 * 
	 * @return the boolean value at the specified index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code index} is negative or is not less
	 *                                   than {@link #size()}
	 */
	boolean getBoolean(int index);

	/**
	 * Returns a new {@code BooleanColumn} containing the specified elements.
	 * 
	 * @param elements the elements to be included in the new column
	 * 
	 * @return a new {@code BooleanColumn} containing the specified elements.
	 */
	public static BooleanColumn of(Boolean... elements) {
		return builder().addAll(elements).build();
	}

	/**
	 * Collects a stream of {@code Booleans} into a new {@code BooleanColumn}.
	 * 
	 * @return a new {@link BooleanColumn}
	 */
	public static Collector<Boolean, ?, BooleanColumn> collector() {
		return Collector.of(BooleanColumn::builder, BooleanColumnBuilder::add, BooleanColumnBuilder::append,
				BooleanColumnBuilder::build);
	}
}
