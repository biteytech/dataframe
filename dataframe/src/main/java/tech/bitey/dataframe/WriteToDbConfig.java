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

import static tech.bitey.dataframe.DfPreconditions.checkArgument;
import static tech.bitey.dataframe.DfPreconditions.checkState;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

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

import tech.bitey.dataframe.db.BooleanToStatement;
import tech.bitey.dataframe.db.ByteToStatement;
import tech.bitey.dataframe.db.DateTimeToStatement;
import tech.bitey.dataframe.db.DateToStatement;
import tech.bitey.dataframe.db.DecimalToStatement;
import tech.bitey.dataframe.db.DoubleToStatement;
import tech.bitey.dataframe.db.FloatToStatement;
import tech.bitey.dataframe.db.IFromResultSet;
import tech.bitey.dataframe.db.IToPreparedStatement;
import tech.bitey.dataframe.db.IntToStatement;
import tech.bitey.dataframe.db.LongToStatement;
import tech.bitey.dataframe.db.ShortToStatement;
import tech.bitey.dataframe.db.StringToStatement;
import tech.bitey.dataframe.db.TimeToStatement;
import tech.bitey.dataframe.db.UuidToStatement;

/**
 * Configuration for writing a dataframe to a database via a
 * {@link PreparedStatement}. The three configurable settings are:
 * <ul>
 * <li>Per-column logic for setting data to the {@code PreparedStatement}. See
 * {@link IToPreparedStatement} for details.
 * <li>Optional batch size. Defaults to {@code 1000}.
 * <li>Whether or not to call commit. Defaults to {@code true}.
 * </ul>
 * 
 * @author biteytech@protonmail.com
 * 
 * @see DataFrameFactory#readFrom(ResultSet, ReadFromDbConfig)
 * @see IFromResultSet
 */
@BeanDefinition(style = "minimal")
public final class WriteToDbConfig implements ImmutableBean {

    public static final WriteToDbConfig DEFAULT_CONFIG = builder().build();

    @PropertyDefinition
    private final List<IToPreparedStatement<?>> toPsLogic;

    @PropertyDefinition
    private final int batchSize;

    @PropertyDefinition
    private final boolean commit;

    @ImmutableDefaults
    private static void applyDefaults(Builder builder) {
        builder.batchSize(1000);
        builder.commit(true);
    }

    @ImmutableValidator
    private void validate() {
        checkArgument(batchSize >= 1, "batch size must be strictly positive");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    void write(DataFrame dataframe, PreparedStatement ps) throws SQLException {

        final Connection conn = ps.getConnection();
        checkState(!conn.getAutoCommit(), "autocommit must be off");

        Column[] columns = dataframe.columns().toArray(new Column[0]);

        IToPreparedStatement[] toPsLogic = this.toPsLogic == null ? null
                : this.toPsLogic.toArray(new IToPreparedStatement[0]);

        if (toPsLogic == null) {
            toPsLogic = new IToPreparedStatement[columns.length];

            for (int i = 0; i < columns.length; i++) {
                switch (columns[i].getType().getCode()) {
                case B:
                    toPsLogic[i] = BooleanToStatement.BOOLEAN_TO_STRING;
                    break;
                case DA:
                    toPsLogic[i] = DateToStatement.DATE_TO_DATE;
                    break;
                case DT:
                    toPsLogic[i] = DateTimeToStatement.DATETIME_TO_TIMESTAMP;
                    break;
                case TI:
                    toPsLogic[i] = TimeToStatement.TIME_TO_STRING;
                    break;
                case BD:
                    toPsLogic[i] = DecimalToStatement.BIGDECIMAL_TO_BIGDECIMAL;
                    break;
                case D:
                    toPsLogic[i] = DoubleToStatement.DOUBLE_TO_DOUBLE;
                    break;
                case F:
                    toPsLogic[i] = FloatToStatement.FLOAT_TO_FLOAT;
                    break;
                case I:
                    toPsLogic[i] = IntToStatement.INT_TO_INT;
                    break;
                case L:
                    toPsLogic[i] = LongToStatement.LONG_TO_LONG;
                    break;
                case T:
                    toPsLogic[i] = ShortToStatement.SHORT_TO_SHORT;
                    break;
                case Y:
                    toPsLogic[i] = ByteToStatement.BYTE_TO_BYTE;
                    break;
                case S:
                    toPsLogic[i] = StringToStatement.STRING_TO_STRING;
                    break;
                case UU:
                    toPsLogic[i] = UuidToStatement.UUID_TO_STRING;
                    break;
                }
            }
        }

        for (int r = 0; r < dataframe.size();) {
            for (int c = 0; c < columns.length; c++)
                toPsLogic[c].set(columns[c], r, ps, c + 1);

            ps.addBatch();

            if (++r % batchSize == 0)
                ps.executeBatch();
        }

        if (dataframe.size() % batchSize != 0)
            ps.executeBatch();

        if (commit)
            conn.commit();
    }

    //------------------------- AUTOGENERATED START -------------------------
    /**
     * The meta-bean for {@code WriteToDbConfig}.
     */
    private static final TypedMetaBean<WriteToDbConfig> META_BEAN =
            MinimalMetaBean.of(
                    WriteToDbConfig.class,
                    new String[] {
                            "toPsLogic",
                            "batchSize",
                            "commit"},
                    () -> new WriteToDbConfig.Builder(),
                    b -> b.getToPsLogic(),
                    b -> b.getBatchSize(),
                    b -> b.isCommit());

    /**
     * The meta-bean for {@code WriteToDbConfig}.
     * @return the meta-bean, not null
     */
    public static TypedMetaBean<WriteToDbConfig> meta() {
        return META_BEAN;
    }

    static {
        MetaBean.register(META_BEAN);
    }

    /**
     * Returns a builder used to create an instance of the bean.
     * @return the builder, not null
     */
    public static WriteToDbConfig.Builder builder() {
        return new WriteToDbConfig.Builder();
    }

    private WriteToDbConfig(
            List<IToPreparedStatement<?>> toPsLogic,
            int batchSize,
            boolean commit) {
        this.toPsLogic = (toPsLogic != null ? Collections.unmodifiableList(new ArrayList<>(toPsLogic)) : null);
        this.batchSize = batchSize;
        this.commit = commit;
        validate();
    }

    @Override
    public TypedMetaBean<WriteToDbConfig> metaBean() {
        return META_BEAN;
    }

    //-----------------------------------------------------------------------
    /**
     * Gets the toPsLogic.
     * @return the value of the property
     */
    public List<IToPreparedStatement<?>> getToPsLogic() {
        return toPsLogic;
    }

    //-----------------------------------------------------------------------
    /**
     * Gets the batchSize.
     * @return the value of the property
     */
    public int getBatchSize() {
        return batchSize;
    }

    //-----------------------------------------------------------------------
    /**
     * Gets the commit.
     * @return the value of the property
     */
    public boolean isCommit() {
        return commit;
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
            WriteToDbConfig other = (WriteToDbConfig) obj;
            return JodaBeanUtils.equal(toPsLogic, other.toPsLogic) &&
                    (batchSize == other.batchSize) &&
                    (commit == other.commit);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = getClass().hashCode();
        hash = hash * 31 + JodaBeanUtils.hashCode(toPsLogic);
        hash = hash * 31 + JodaBeanUtils.hashCode(batchSize);
        hash = hash * 31 + JodaBeanUtils.hashCode(commit);
        return hash;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder(128);
        buf.append("WriteToDbConfig{");
        buf.append("toPsLogic").append('=').append(toPsLogic).append(',').append(' ');
        buf.append("batchSize").append('=').append(batchSize).append(',').append(' ');
        buf.append("commit").append('=').append(JodaBeanUtils.toString(commit));
        buf.append('}');
        return buf.toString();
    }

    //-----------------------------------------------------------------------
    /**
     * The bean-builder for {@code WriteToDbConfig}.
     */
    public static final class Builder extends DirectFieldsBeanBuilder<WriteToDbConfig> {

        private List<IToPreparedStatement<?>> toPsLogic;
        private int batchSize;
        private boolean commit;

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
        private Builder(WriteToDbConfig beanToCopy) {
            this.toPsLogic = (beanToCopy.getToPsLogic() != null ? new ArrayList<>(beanToCopy.getToPsLogic()) : null);
            this.batchSize = beanToCopy.getBatchSize();
            this.commit = beanToCopy.isCommit();
        }

        //-----------------------------------------------------------------------
        @Override
        public Object get(String propertyName) {
            switch (propertyName.hashCode()) {
                case -1976069088:  // toPsLogic
                    return toPsLogic;
                case -978846117:  // batchSize
                    return batchSize;
                case -1354815177:  // commit
                    return commit;
                default:
                    throw new NoSuchElementException("Unknown property: " + propertyName);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public Builder set(String propertyName, Object newValue) {
            switch (propertyName.hashCode()) {
                case -1976069088:  // toPsLogic
                    this.toPsLogic = (List<IToPreparedStatement<?>>) newValue;
                    break;
                case -978846117:  // batchSize
                    this.batchSize = (Integer) newValue;
                    break;
                case -1354815177:  // commit
                    this.commit = (Boolean) newValue;
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
        public WriteToDbConfig build() {
            return new WriteToDbConfig(
                    toPsLogic,
                    batchSize,
                    commit);
        }

        //-----------------------------------------------------------------------
        /**
         * Sets the toPsLogic.
         * @param toPsLogic  the new value
         * @return this, for chaining, not null
         */
        public Builder toPsLogic(List<IToPreparedStatement<?>> toPsLogic) {
            this.toPsLogic = toPsLogic;
            return this;
        }

        /**
         * Sets the {@code toPsLogic} property in the builder
         * from an array of objects.
         * @param toPsLogic  the new value
         * @return this, for chaining, not null
         */
        @SafeVarargs
        public final Builder toPsLogic(IToPreparedStatement<?>... toPsLogic) {
            return toPsLogic(Arrays.asList(toPsLogic));
        }

        /**
         * Sets the batchSize.
         * @param batchSize  the new value
         * @return this, for chaining, not null
         */
        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /**
         * Sets the commit.
         * @param commit  the new value
         * @return this, for chaining, not null
         */
        public Builder commit(boolean commit) {
            this.commit = commit;
            return this;
        }

        //-----------------------------------------------------------------------
        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder(128);
            buf.append("WriteToDbConfig.Builder{");
            buf.append("toPsLogic").append('=').append(JodaBeanUtils.toString(toPsLogic)).append(',').append(' ');
            buf.append("batchSize").append('=').append(JodaBeanUtils.toString(batchSize)).append(',').append(' ');
            buf.append("commit").append('=').append(JodaBeanUtils.toString(commit));
            buf.append('}');
            return buf.toString();
        }

    }

    //-------------------------- AUTOGENERATED END --------------------------
}
