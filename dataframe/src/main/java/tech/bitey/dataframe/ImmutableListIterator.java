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

import java.util.ListIterator;

interface ImmutableListIterator<E> extends ListIterator<E> {

	@Override
	default void remove() {
		throw new UnsupportedOperationException("remove");
	}
	
	@Override
	default void set(E e) {
		throw new UnsupportedOperationException("set");
	}
	
	@Override
	default void add(E e) {
		throw new UnsupportedOperationException("add");
	}
}
