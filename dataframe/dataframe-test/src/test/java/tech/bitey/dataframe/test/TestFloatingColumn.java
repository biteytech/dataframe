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

import java.util.List;
import java.util.function.IntFunction;

abstract class TestFloatingColumn<E extends Comparable<E>> extends TestColumn<E> {

	TestFloatingColumn(E min, E max, IntFunction<E[]> createArray) {
		super(min, max, createArray);
	}

	@Override
	List<TestSample<E>> baseSamples() {

		List<TestSample<E>> samples = super.baseSamples();

		samples.add(wrapSample("singleNaN", singleNaN()));
		samples.add(wrapSample("duoNaN", duoNaN()));
		samples.add(wrapSample("nonFinite", nonFinite()));

		return samples;
	}

	abstract E[] singleNaN();

	abstract E[] duoNaN();

	abstract E[] nonFinite();
}
