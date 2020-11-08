/*
 * Copyright 2020 biteytech@protonmail.com
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

import static java.lang.Character.isLetterOrDigit;
import static java.nio.charset.StandardCharsets.UTF_8;
import static tech.bitey.dataframe.DfPreconditions.checkArgument;
import static tech.bitey.dataframe.DfPreconditions.checkNotNull;
import static tech.bitey.dataframe.DfPreconditions.checkState;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

import org.joda.beans.ImmutableBean;
import org.joda.beans.JodaBeanUtils;
import org.joda.beans.MetaBean;
import org.joda.beans.MetaProperty;
import org.joda.beans.TypedMetaBean;
import org.joda.beans.gen.BeanDefinition;
import org.joda.beans.gen.ImmutableDefaults;
import org.joda.beans.gen.ImmutableValidator;
import org.joda.beans.gen.PropertyDefinition;
import org.joda.beans.impl.direct.DirectFieldsBeanBuilder;
import org.joda.beans.impl.direct.MinimalMetaBean;

/**
 * Configuration for reading a dataframe from a CSV file. The configurable
 * settings are:
 * <ul>
 * <li>Column types - this is mandatory as there is currently no logic to
 * auto-detect column types.
 * <li>Column names - required if and only if there is no header line.
 * <li>Column parsers - optional, defaults to using
 * {@link ColumnType#parse(String)} for each column. Elements can be null, in
 * which case the default parsing will be applied for the corresponding column.
 * <li>Delimiter - defaults to comma
 * </ul>
 * 
 * @author biteytech@protonmail.com
 */
@BeanDefinition(style = "minimal")
public final class ReadCsvConfig implements ImmutableBean {

    /**
     * {@link ColumnType column types}. The size of this list must match the number
     * of fields in the CSV file.
     */
    @PropertyDefinition(validate = "notNull")
    private final List<ColumnType<?>> columnTypes;

    /**
     * Must be specified if and only if a header line is not present in the CSV
     * file.
     */
    @PropertyDefinition
    private final List<String> columnNames;

    /**
     * Optional, defaults to using {@link ColumnType#parse(String)} for each column.
     * Elements can be null, in which case the default parsing will be applied for
     * the corresponding column.
     */
    @PropertyDefinition
    private final List<Function<String, ?>> columnParsers;

    /**
     * any ASCII character which is not a letter, digit, double quote, CR, or LF
     */
    @PropertyDefinition
    private final char delim;

    @ImmutableDefaults
    private static void applyDefaults(Builder builder) {
        builder.delim(',');
    }

    @ImmutableValidator
    private void validate() {

        checkArgument(columnTypes.size() > 0, "columnTypes array cannot be empty");

        if (columnNames != null)
            checkArgument(columnNames.size() == columnTypes.size(),
                    "columnTypes and columnNames must have the same length");

        if (columnParsers != null)
            checkArgument(columnParsers.size() == columnTypes.size(),
                    "columnParsers and columnNames must have the same length");

        checkArgument(delim <= 0x7F && !isLetterOrDigit(delim) && delim != '"' && delim != '\r' && delim != '\n',
                "delimiter must an ASCII character which is not a letter, digit, double quote, CR, or LF");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    DataFrame process(InputStream is) throws IOException {

        final ColumnBuilder[] builders = columnTypes.stream().map(ColumnType::builder).toArray(ColumnBuilder[]::new);
        final String[] columnNames;

        try (BufferedReader in = new BufferedReader(new InputStreamReader(is, UTF_8))) {

            int[] lineno = { 0 };
            int rno = 1;

            if (this.columnNames == null) {
                columnNames = nextRecord(in, rno, lineno, true);
                rno++;

                checkNotNull(columnNames, "missing header - no column names configured and empty input");
                checkState(columnNames.length == builders.length, "mismatch between number of fields in header ("
                        + columnNames.length + "), vs configured types (" + builders.length + ")");
            } else {
                columnNames = this.columnNames.toArray(new String[0]);
            }

            final Function<String, ?>[] columnParsers = this.columnParsers == null ? new Function[columnTypes.size()]
                    : this.columnParsers.toArray(new Function[0]);
            for (int i = 0; i < columnParsers.length; i++)
                if (columnParsers[i] == null)
                    columnParsers[i] = columnTypes.get(i)::parse;

            for (String[] fields; (fields = nextRecord(in, rno, lineno, false)) != null; rno++) {
                try {
                    checkState(fields.length == builders.length, "mismatch between number of fields (" + fields.length
                            + "), vs configured types (" + builders.length + ")");

                    for (int i = 0; i < fields.length; i++) {
                        try {
                            if (fields[i] == null)
                                builders[i].addNull();
                            else {
                                Object value = columnParsers[i].apply(fields[i]);
                                builders[i].add(value);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(errorMessage(i + 1, e.getMessage()), e);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(errorMessage(rno, lineno[0], e.getMessage()), e);
                }
            }
        }

        Column<?>[] columns = new Column<?>[builders.length];
        for (int i = 0; i < columns.length; i++)
            columns[i] = builders[i].build();

        return DataFrameFactory.create(columns, columnNames);
    }

    private String[] nextRecord(BufferedReader in, int rno, int lineno[], boolean header) {
        try {
            int count = 0;
            StringBuilder builder = null;

            do {
                String line = in.readLine();
                lineno[0]++;

                if (line == null) {
                    if (builder == null)
                        return null;
                    else
                        throw new IllegalStateException("reached EOF with unmatched quote");
                }

                // count double quotes
                for (int i = 0; i < line.length(); i++)
                    if (line.charAt(i) == '"')
                        count++;

                if (builder == null && count % 2 == 0) {
                    // first line is a whole record
                    return split(line, header);
                } else if (builder == null) {
                    // first line is not a whole record
                    builder = new StringBuilder();
                    builder.append(line);
                } else {
                    // subsequent lines always appended
                    builder.append('\n');
                    builder.append(line);
                }
            } while (count % 2 == 1);

            return split(builder.toString(), header);
        } catch (Exception e) {
            throw new RuntimeException(errorMessage(rno, lineno[0], e.getMessage()), e);
        }
    }

    private static String errorMessage(int rno, int lineno, String error) {
        return String.format("Record #%d: Line #%d: %s", rno, lineno, error);
    }

    private static String errorMessage(int fno, String error) {
        return String.format("Field #%d: %s", fno, error);
    }

    private String[] split(String record, boolean header) {

        List<Integer> splits = new ArrayList<>();

        for (int i = 0, count = 0; i < record.length(); i++) {

            final char c = record.charAt(i);

            if (c == '"')
                count++;
            else if (c == delim && count % 2 == 0)
                splits.add(i);
        }

        int prevSplit = -1, i = 0;
        String[] fields = new String[splits.size() + 1];
        for (int split : splits) {
            fields[i++] = record.substring(prevSplit + 1, split);
            prevSplit = split;
        }
        fields[i] = record.substring(prevSplit + 1);

        for (i = 0; i < fields.length; i++) {
            if ("\"\"".equals(fields[i]) && (header || columnTypes.get(i) == ColumnType.STRING)) {
                fields[i] = "";
                continue;
            }

            if (fields[i].startsWith("\""))
                fields[i] = fields[i].substring(1, fields[i].length() - 1);

            if (fields[i].isEmpty()) {
                fields[i] = null;
                continue;
            }

            int count = 0;
            for (int j = 0; j < fields[i].length(); j++) {
                if (fields[i].charAt(j) == '"')
                    count++;
                else if (count > 0) {
                    if (count % 2 == 0)
                        count = 0;
                    else
                        throw new RuntimeException(errorMessage(i + 1, "unescaped \""));
                }
            }

            fields[i] = fields[i].replace("\"\"", "\"");
        }

        return fields;
    }

    //------------------------- AUTOGENERATED START -------------------------
    /**
     * The meta-bean for {@code ReadCsvConfig}.
     */
    private static final TypedMetaBean<ReadCsvConfig> META_BEAN =
            MinimalMetaBean.of(
                    ReadCsvConfig.class,
                    new String[] {
                            "columnTypes",
                            "columnNames",
                            "columnParsers",
                            "delim"},
                    () -> new ReadCsvConfig.Builder(),
                    b -> b.getColumnTypes(),
                    b -> b.getColumnNames(),
                    b -> b.getColumnParsers(),
                    b -> b.getDelim());

    /**
     * The meta-bean for {@code ReadCsvConfig}.
     * @return the meta-bean, not null
     */
    public static TypedMetaBean<ReadCsvConfig> meta() {
        return META_BEAN;
    }

    static {
        MetaBean.register(META_BEAN);
    }

    /**
     * Returns a builder used to create an instance of the bean.
     * @return the builder, not null
     */
    public static ReadCsvConfig.Builder builder() {
        return new ReadCsvConfig.Builder();
    }

    private ReadCsvConfig(
            List<ColumnType<?>> columnTypes,
            List<String> columnNames,
            List<Function<String, ?>> columnParsers,
            char delim) {
        JodaBeanUtils.notNull(columnTypes, "columnTypes");
        this.columnTypes = Collections.unmodifiableList(new ArrayList<>(columnTypes));
        this.columnNames = (columnNames != null ? Collections.unmodifiableList(new ArrayList<>(columnNames)) : null);
        this.columnParsers = (columnParsers != null ? Collections.unmodifiableList(new ArrayList<>(columnParsers)) : null);
        this.delim = delim;
        validate();
    }

    @Override
    public TypedMetaBean<ReadCsvConfig> metaBean() {
        return META_BEAN;
    }

    //-----------------------------------------------------------------------
    /**
     * Gets {@link ColumnType column types}. The size of this list must match the number
     * of fields in the CSV file.
     * @return the value of the property, not null
     */
    public List<ColumnType<?>> getColumnTypes() {
        return columnTypes;
    }

    //-----------------------------------------------------------------------
    /**
     * Gets must be specified if and only if a header line is not present in the CSV
     * file.
     * @return the value of the property
     */
    public List<String> getColumnNames() {
        return columnNames;
    }

    //-----------------------------------------------------------------------
    /**
     * Gets optional, defaults to using {@link ColumnType#parse(String)} for each column.
     * Elements can be null, in which case the default parsing will be applied for
     * the corresponding column.
     * @return the value of the property
     */
    public List<Function<String, ?>> getColumnParsers() {
        return columnParsers;
    }

    //-----------------------------------------------------------------------
    /**
     * Gets any ASCII character which is not a letter, digit, double quote, CR, or LF
     * @return the value of the property
     */
    public char getDelim() {
        return delim;
    }

    //-----------------------------------------------------------------------
    /**
     * Returns a builder that allows this bean to be mutated.
     * @return the mutable builder, not null
     */
    public Builder toBuilder() {
        return new Builder(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj != null && obj.getClass() == this.getClass()) {
            ReadCsvConfig other = (ReadCsvConfig) obj;
            return JodaBeanUtils.equal(columnTypes, other.columnTypes) &&
                    JodaBeanUtils.equal(columnNames, other.columnNames) &&
                    JodaBeanUtils.equal(columnParsers, other.columnParsers) &&
                    (delim == other.delim);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = getClass().hashCode();
        hash = hash * 31 + JodaBeanUtils.hashCode(columnTypes);
        hash = hash * 31 + JodaBeanUtils.hashCode(columnNames);
        hash = hash * 31 + JodaBeanUtils.hashCode(columnParsers);
        hash = hash * 31 + JodaBeanUtils.hashCode(delim);
        return hash;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder(160);
        buf.append("ReadCsvConfig{");
        buf.append("columnTypes").append('=').append(columnTypes).append(',').append(' ');
        buf.append("columnNames").append('=').append(columnNames).append(',').append(' ');
        buf.append("columnParsers").append('=').append(columnParsers).append(',').append(' ');
        buf.append("delim").append('=').append(JodaBeanUtils.toString(delim));
        buf.append('}');
        return buf.toString();
    }

    //-----------------------------------------------------------------------
    /**
     * The bean-builder for {@code ReadCsvConfig}.
     */
    public static final class Builder extends DirectFieldsBeanBuilder<ReadCsvConfig> {

        private List<ColumnType<?>> columnTypes = Collections.emptyList();
        private List<String> columnNames;
        private List<Function<String, ?>> columnParsers;
        private char delim;

        /**
         * Restricted constructor.
         */
        private Builder() {
            applyDefaults(this);
        }

        /**
         * Restricted copy constructor.
         * @param beanToCopy  the bean to copy from, not null
         */
        private Builder(ReadCsvConfig beanToCopy) {
            this.columnTypes = new ArrayList<>(beanToCopy.getColumnTypes());
            this.columnNames = (beanToCopy.getColumnNames() != null ? new ArrayList<>(beanToCopy.getColumnNames()) : null);
            this.columnParsers = (beanToCopy.getColumnParsers() != null ? new ArrayList<>(beanToCopy.getColumnParsers()) : null);
            this.delim = beanToCopy.getDelim();
        }

        //-----------------------------------------------------------------------
        @Override
        public Object get(String propertyName) {
            switch (propertyName.hashCode()) {
                case -844743997:  // columnTypes
                    return columnTypes;
                case -851002990:  // columnNames
                    return columnNames;
                case 9945086:  // columnParsers
                    return columnParsers;
                case 95468143:  // delim
                    return delim;
                default:
                    throw new NoSuchElementException("Unknown property: " + propertyName);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public Builder set(String propertyName, Object newValue) {
            switch (propertyName.hashCode()) {
                case -844743997:  // columnTypes
                    this.columnTypes = (List<ColumnType<?>>) newValue;
                    break;
                case -851002990:  // columnNames
                    this.columnNames = (List<String>) newValue;
                    break;
                case 9945086:  // columnParsers
                    this.columnParsers = (List<Function<String, ?>>) newValue;
                    break;
                case 95468143:  // delim
                    this.delim = (Character) newValue;
                    break;
                default:
                    throw new NoSuchElementException("Unknown property: " + propertyName);
            }
            return this;
        }

        @Override
        public Builder set(MetaProperty<?> property, Object value) {
            super.set(property, value);
            return this;
        }

        @Override
        public ReadCsvConfig build() {
            return new ReadCsvConfig(
                    columnTypes,
                    columnNames,
                    columnParsers,
                    delim);
        }

        //-----------------------------------------------------------------------
        /**
         * Sets {@link ColumnType column types}. The size of this list must match the number
         * of fields in the CSV file.
         * @param columnTypes  the new value, not null
         * @return this, for chaining, not null
         */
        public Builder columnTypes(List<ColumnType<?>> columnTypes) {
            JodaBeanUtils.notNull(columnTypes, "columnTypes");
            this.columnTypes = columnTypes;
            return this;
        }

        /**
         * Sets the {@code columnTypes} property in the builder
         * from an array of objects.
         * @param columnTypes  the new value, not null
         * @return this, for chaining, not null
         */
        @SafeVarargs
        public final Builder columnTypes(ColumnType<?>... columnTypes) {
            return columnTypes(Arrays.asList(columnTypes));
        }

        /**
         * Sets must be specified if and only if a header line is not present in the CSV
         * file.
         * @param columnNames  the new value
         * @return this, for chaining, not null
         */
        public Builder columnNames(List<String> columnNames) {
            this.columnNames = columnNames;
            return this;
        }

        /**
         * Sets the {@code columnNames} property in the builder
         * from an array of objects.
         * @param columnNames  the new value
         * @return this, for chaining, not null
         */
        public Builder columnNames(String... columnNames) {
            return columnNames(Arrays.asList(columnNames));
        }

        /**
         * Sets optional, defaults to using {@link ColumnType#parse(String)} for each column.
         * Elements can be null, in which case the default parsing will be applied for
         * the corresponding column.
         * @param columnParsers  the new value
         * @return this, for chaining, not null
         */
        public Builder columnParsers(List<Function<String, ?>> columnParsers) {
            this.columnParsers = columnParsers;
            return this;
        }

        /**
         * Sets the {@code columnParsers} property in the builder
         * from an array of objects.
         * @param columnParsers  the new value
         * @return this, for chaining, not null
         */
        @SafeVarargs
        public final Builder columnParsers(Function<String, ?>... columnParsers) {
            return columnParsers(Arrays.asList(columnParsers));
        }

        /**
         * Sets any ASCII character which is not a letter, digit, double quote, CR, or LF
         * @param delim  the new value
         * @return this, for chaining, not null
         */
        public Builder delim(char delim) {
            this.delim = delim;
            return this;
        }

        //-----------------------------------------------------------------------
        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder(160);
            buf.append("ReadCsvConfig.Builder{");
            buf.append("columnTypes").append('=').append(JodaBeanUtils.toString(columnTypes)).append(',').append(' ');
            buf.append("columnNames").append('=').append(JodaBeanUtils.toString(columnNames)).append(',').append(' ');
            buf.append("columnParsers").append('=').append(JodaBeanUtils.toString(columnParsers)).append(',').append(' ');
            buf.append("delim").append('=').append(JodaBeanUtils.toString(delim));
            buf.append('}');
            return buf.toString();
        }

    }

    //-------------------------- AUTOGENERATED END --------------------------
}
