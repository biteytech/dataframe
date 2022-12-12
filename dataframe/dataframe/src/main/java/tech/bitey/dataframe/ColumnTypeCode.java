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

/**
 * Each {@link ColumnType} has a corresponding short code.
 * <ul>
 * <li>{@link #B} - Boolean
 * <li>{@link #DA} - Date
 * <li>{@link #DT} - DateTime
 * <li>{@link #TI} - Time
 * <li>{@link #IN} - Instant
 * <li>{@link #D} - Double
 * <li>{@link #F} - Float
 * <li>{@link #I} - Int
 * <li>{@link #L} - Long
 * <li>{@link #T} - Short
 * <li>{@link #Y} - Byte
 * <li>{@link #S} - String
 * <li>{@link #BD} - Decimal
 * <li>{@link #UU} - UUID
 * <li>{@link #NS} - NormalString
 * <li>{@link #BL} - Blob
 * <li>{@link #FS} - FixedAscii
 * </ul>
 * 
 * @author biteytech@protonmail.com
 */
public enum ColumnTypeCode {

	/** The type code for {@link BooleanColumn} */
	B, // Boolean
	/** The type code for {@link DateColumn} */
	DA, // Date
	/** The type code for {@link DateTimeColumn} */
	DT, // DateTime
	/** The type code for {@link TimeColumn} */
	TI, // Time
	/** The type code for {@link InstantColumn} */
	IN, // Instant
	/** The type code for {@link DoubleColumn} */
	D, // Double
	/** The type code for {@link FloatColumn} */
	F, // Float
	/** The type code for {@link IntColumn} */
	I, // Int
	/** The type code for {@link LongColumn} */
	L, // Long
	/** The type code for {@link ShortColumn} */
	T, // Short
	/** The type code for {@link ByteColumn} */
	Y, // Byte
	/** The type code for {@link StringColumn} */
	S, // String
	/** The type code for {@link DecimalColumn} */
	BD, // Decimal
	/** The type code for {@link UuidColumn} */
	UU, // UUID
	/** The type code for {@link NormalStringColumn} */
	NS, // Normal String
	/** The type code for {@link BlobColumn} */
	BL, // Blob
	/** The type code for {@link FixedAsciiColumn} */
	FS, // Fixed Ascii String
	;

	public ColumnType<?> getType() {
		return switch (this) {
		case B -> ColumnType.BOOLEAN;
		case DA -> ColumnType.DATE;
		case DT -> ColumnType.DATETIME;
		case TI -> ColumnType.TIME;
		case IN -> ColumnType.INSTANT;
		case D -> ColumnType.DOUBLE;
		case F -> ColumnType.FLOAT;
		case I -> ColumnType.INT;
		case L -> ColumnType.LONG;
		case T -> ColumnType.SHORT;
		case Y -> ColumnType.BYTE;
		case S -> ColumnType.STRING;
		case BD -> ColumnType.DECIMAL;
		case UU -> ColumnType.UUID;
		case NS -> ColumnType.NSTRING;
		case BL -> ColumnType.BLOB;
		case FS -> ColumnType.FSTRING;
		};
	}
}
