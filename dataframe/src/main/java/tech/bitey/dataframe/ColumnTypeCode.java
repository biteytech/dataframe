package tech.bitey.dataframe;

public enum ColumnTypeCode {

	B, // Boolean
	DA, // Date
	DT, // DateTime
	D, // Double
	F, // Float
	I, // Int
	L, // Long
	T, // Short
	Y, // Byte
	S, // String
	BD; // Decimal

	ColumnType<?> getType() {
		switch (this) {
		case B:
			return ColumnType.BOOLEAN;
		case DA:
			return ColumnType.DATE;
		case DT:
			return ColumnType.DATETIME;
		case D:
			return ColumnType.DOUBLE;
		case F:
			return ColumnType.FLOAT;
		case I:
			return ColumnType.INT;
		case L:
			return ColumnType.LONG;
		case T:
			return ColumnType.SHORT;
		case Y:
			return ColumnType.BYTE;
		case S:
			return ColumnType.STRING;
		case BD:
			return ColumnType.DECIMAL;
		}
		throw new IllegalStateException();
	}
}
