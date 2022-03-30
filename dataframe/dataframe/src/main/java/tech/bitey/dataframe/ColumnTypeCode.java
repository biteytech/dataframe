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

public enum ColumnTypeCode {

	B, // Boolean
	DA, // Date
	DT, // DateTime
	TI, // Time
	IN, // Instant
	D, // Double
	F, // Float
	I, // Int
	L, // Long
	T, // Short
	Y, // Byte
	S, // String
	BD, // Decimal
	UU, // UUID
	NS, // Normal String
	BL, // Blob
	;

	ColumnType<?> getType() {
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
		};
	}
}
