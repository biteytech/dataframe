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

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.util.Spliterator.NONNULL;
import static tech.bitey.dataframe.AbstractColumn.readInt;
import static tech.bitey.dataframe.guava.DfPreconditions.checkState;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import tech.bitey.bufferstuff.BufferBitSet;

/**
 * Represents the possible element types supported by the concrete
 * {@link Column} implementation. One of:
 * <ul>
 * <li>{@link #BOOLEAN}
 * <li>{@link #DATE}
 * <li>{@link #DATETIME}
 * <li>{@link #DOUBLE}
 * <li>{@link #FLOAT}
 * <li>{@link #INT}
 * <li>{@link #LONG}
 * <li>{@link #STRING}
 * </ul>
 * 
 * @author biteytech@protonmail.com
 */
public enum ColumnType {

	/** The type for {@link BooleanColumn} */
	BOOLEAN("B") {
	},
	/** The type for {@link DateColumn} */
	DATE("DA") {
	},
	/** The type for {@link DateTimeColumn} */
	DATETIME("DT") {
	},
	/** The type for {@link DoubleColumn} */
	DOUBLE("D") {
	},
	/** The type for {@link FloatColumn} */
	FLOAT("F") {
	},
	/** The type for {@link IntColumn} */
	INT("I") {
	},
	/** The type for {@link LongColumn} */
	LONG("L") {
	},
	/** The type for {@link StringColumn} */
	STRING("S") {
	},;

	private final String code;

	private ColumnType(String code) {

		byte[] codeBytes = code.getBytes();
		checkState(codeBytes.length >= 1 && codeBytes.length <= 2, "code must be one or two (ascii) characters");

		this.code = code;
	}

	String getCode() {
		return code;
	}

	/**
	 * Returns a {@link ColumnBuilder builder} for this column type.
	 * 
	 * @param <T> the corresponding element type
	 * 
	 * @return a {@link ColumnBuilder builder} for this column type.
	 */
	public <T> ColumnBuilder<T> builder() {
		return builder(0);
	}

	/**
	 * Returns a {@link ColumnBuilder builder} for this column type with the
	 * specified characteristic.
	 * 
	 * @param characteristic - one of:
	 *                       <ul>
	 *                       <li>{@code 0} (zero) - no constraints on the elements
	 *                       to be added to the column
	 *                       <li>{@link java.util.Spliterator#NONNULL NONNULL}
	 *                       <li>{@link java.util.Spliterator#SORTED SORTED}
	 *                       <li>{@link java.util.Spliterator#DISTINCT DISTINCT}
	 *                       </ul>
	 * 
	 * @param <T>            the column's element type
	 * 
	 * @return a {@link ColumnBuilder builder} for this column type.
	 * 
	 * @throws IllegalArgumentException if {@code characteristic} is not valid
	 */
	@SuppressWarnings("unchecked")
	public <T> ColumnBuilder<T> builder(int characteristic) {
		switch (this) {
		case BOOLEAN:
			return (ColumnBuilder<T>) BooleanColumn.builder();
		case DATE:
			return (ColumnBuilder<T>) DateColumn.builder(characteristic);
		case DATETIME:
			return (ColumnBuilder<T>) DateTimeColumn.builder(characteristic);
		case DOUBLE:
			return (ColumnBuilder<T>) DoubleColumn.builder(characteristic);
		case FLOAT:
			return (ColumnBuilder<T>) FloatColumn.builder(characteristic);
		case INT:
			return (ColumnBuilder<T>) IntColumn.builder(characteristic);
		case LONG:
			return (ColumnBuilder<T>) LongColumn.builder(characteristic);
		case STRING:
			return (ColumnBuilder<T>) StringColumn.builder(characteristic);
		}

		throw new IllegalStateException();
	}

	/**
	 * Returns a {@link Column} of the corresponding type that only contains nulls.
	 * 
	 * @param size the number of nulls in the column
	 * 
	 * @return a {@code Column} of the corresponding type that only contains nulls.
	 */
	public Column<?> nullColumn(int size) {
		return builder().addNulls(size).build();
	}
	
	Column<?> readFrom(ReadableByteChannel channel, int characteristics) throws IOException {
		BufferBitSet nonNulls = null;
		int size = 0;
		if(!((characteristics & NONNULL) != 0)) {
			size = readInt(channel, BIG_ENDIAN);
			nonNulls = BufferBitSet.readFrom(channel);
		}
		
		switch (this) {
		case BOOLEAN: {
			NonNullBooleanColumn column = NonNullBooleanColumn.EMPTY.readFrom(channel);
			if(nonNulls == null)
				return column;
			else
				return new NullableBooleanColumn(column, nonNulls, null, 0, size);
		}
		case DATE: {
			NonNullDateColumn column = NonNullDateColumn.empty(characteristics).readFrom(channel);
			if(nonNulls == null)
				return column;
			else
				return new NullableDateColumn(column, nonNulls, null, 0, size);
		}
		case DATETIME: {
			NonNullDateTimeColumn column = NonNullDateTimeColumn.empty(characteristics).readFrom(channel);
			if(nonNulls == null)
				return column;
			else
				return new NullableDateTimeColumn(column, nonNulls, null, 0, size);
		}
		case DOUBLE: {
			NonNullDoubleColumn column = NonNullDoubleColumn.empty(characteristics).readFrom(channel);
			if(nonNulls == null)
				return column;
			else
				return new NullableDoubleColumn(column, nonNulls, null, 0, size);
		}
		case FLOAT: {
			NonNullFloatColumn column = NonNullFloatColumn.empty(characteristics).readFrom(channel);
			if(nonNulls == null)
				return column;
			else
				return new NullableFloatColumn(column, nonNulls, null, 0, size);
		}
		case INT: {
			NonNullIntColumn column = NonNullIntColumn.empty(characteristics).readFrom(channel);
			if(nonNulls == null)
				return column;
			else
				return new NullableIntColumn(column, nonNulls, null, 0, size);
		}
		case LONG: {
			NonNullLongColumn column = NonNullLongColumn.empty(characteristics).readFrom(channel);
			if(nonNulls == null)
				return column;
			else
				return new NullableLongColumn(column, nonNulls, null, 0, size);
		}
		case STRING: {
			NonNullStringColumn column = NonNullStringColumn.empty(characteristics).readFrom(channel);
			if(nonNulls == null)
				return column;
			else
				return new NullableStringColumn(column, nonNulls, null, 0, size);
		}
		}

		throw new IllegalStateException();
	}
}
