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

import java.util.List;

public record GroupByConfig(List<String> groupByNames, List<String> derivedNames, List<ColumnType<?>> derivedTypes,
		List<GroupByReduction> reductions) {

	public GroupByConfig {

		Pr.checkState(derivedNames.size() == derivedTypes.size(), "derivedNames.size() != derivedTypes.size()");
		Pr.checkState(derivedNames.size() == reductions.size(), "derivedNames.size() != reductions.size()");

		groupByNames = List.copyOf(groupByNames);
		derivedNames = List.copyOf(derivedNames);
		derivedTypes = List.copyOf(derivedTypes);
		reductions = List.copyOf(reductions);
	}

	public GroupByConfig(List<String> groupByNames) {
		this(groupByNames, List.of(), List.of(), List.of());
	}
}
