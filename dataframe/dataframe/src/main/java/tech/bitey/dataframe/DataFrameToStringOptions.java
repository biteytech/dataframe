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

import static tech.bitey.dataframe.Pr.checkArgument;

/**
 * Options for how to render to plain text.
 * <ul>
 * <li><b>maxRows</b> - Pretty-print the first {@code ⌈maxRows/2⌉} and last
 * {@code ⌊maxRows/2⌋} rows.
 * <li><b>typesInHeader</b> - include type information in the header
 * </ul>
 * 
 * @author biteytech@protonmail.com
 */
public record DataFrameToStringOptions(int maxRows, boolean typesInHeader) {

	public DataFrameToStringOptions {
		checkArgument(maxRows >= 0, "maxRows cannot be negative");
	}
}
