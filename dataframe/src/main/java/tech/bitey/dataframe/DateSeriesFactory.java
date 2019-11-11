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

import static tech.bitey.dataframe.DfPreconditions.checkArgument;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

/**
 * Factory methods for creating {@link DateSeries DateSeries}.
 * 
 * @author biteytech@protonmail.com
 */
public enum DateSeriesFactory {
	;

	static final String DEFAULT_DATE_COLUMN_NAME = "DATES";
	static final String DEFAULT_VALUE_COLUMN_NAME = "VALUES";

	/**
	 * Returns a {@link DateSeries} with the specified dates and values, using the
	 * default column names: "DATES" and "VALUES"
	 * 
	 * @param dates  - the dates
	 * @param values - the values
	 * 
	 * @return a {@code DateSeries} with the specified dates and values.
	 */
	public static DateSeries create(DateColumn dates, DoubleColumn values) {
		return create(dates, DEFAULT_DATE_COLUMN_NAME, values, DEFAULT_VALUE_COLUMN_NAME);
	}

	/**
	 * Returns a {@link DateSeries} with the specified dates, values, and column
	 * names.
	 * 
	 * @param dates           - the dates
	 * @param dateColumnName  - the date column name
	 * @param values          - the values
	 * @param valueColumnName - the value column name
	 * 
	 * @return a {@code DateSeries} with the specified dates and values.
	 */
	public static DateSeries create(DateColumn dates, String dateColumnName, DoubleColumn values,
			String valueColumnName) {

		checkArgument(dates.isDistinct(), "dates column must be a unique index (distinct values)");
		checkArgument(values.isNonnull(), "values column must be non-null");

		return new DateSeriesImpl((NonNullDateColumn) dates, dateColumnName, (NonNullDoubleColumn) values,
				valueColumnName);
	}

	/**
	 * Load a time series from a file created via {@link DataFrame#writeTo(File)}.
	 * 
	 * @param file - the file to read from
	 * 
	 * @return the time series loaded from the specified file
	 * 
	 * @throws IOException if some I/O error occurs
	 */
	public static DateSeries readFrom(File file) throws IOException {
		return DataFrameFactory.readFrom(file).toDateSeries(1);
	}

	/**
	 * Read a time series from the specified {@link ReadableByteChannel}.
	 * 
	 * @param channel - the channel to read from
	 * 
	 * @return the time series read from the specified channel
	 * 
	 * @throws IOException if some I/O error occurs
	 */
	public static DateSeries readFrom(ReadableByteChannel channel) throws IOException {
		return DataFrameFactory.readFrom(channel).toDateSeries(1);
	}
}
