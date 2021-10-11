# dataframe

[![Apache 2.0](https://img.shields.io/github/license/nebula-plugins/nebula-project-plugin.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Build Status](https://api.travis-ci.com/biteytech/dataframe.svg?branch=master)](https://app.travis-ci.com/github/biteytech/dataframe)

Another dataframe library for Java, inspired by Tablesaw, built on nio buffers.

To add a dependency on dataframe using Maven, use the following:

```xml
<dependency>
  <groupId>tech.bitey</groupId>
  <artifactId>dataframe</artifactId>
  <version>1.1.1</version>
</dependency>
```

Requires Java 11 or higher.

### What's different about this particular data frame library?
* It's geared towards making it easier to ship around tabular data for Java backend developers - rather than for data science. This is not Pandas for Java.
* Data is stored in ByteBuffers, so the data frames can read/write to Channels with minimal overhead (save to files, send over network).
* Optimized for space. For example, Booleans take one bit each, DateTimes take one long (microsecond precision).
* Nulls are stored in a separate BitSet (also backed by ByteBuffer), taking up one bit per Column length (no extra space is used if all values are non-null).

### Features
* Supports the most common types: String, Integer, Long, Short, Byte, Boolean, Double, Float, Date, DateTime, and BigDecimal (and Time and UUID).
* Column and DataFrame are immutable. Columns can be created from collections, arrays, streams, or with builders.
* Read/write to File or Channel with minimal overhead
* Read/write CSV files
* Read from ResultSet, write with PreparedStatement
* Backing ByteBuffers can be on heap or off (as a global config)
* Column implements List. DataFrame implements `List<Row>`. If the DataFrame has a key column it can be viewed as a `NavigableMap<KEY_TYPE, Row>`
* Basic filtering, joining, grouping

### Sample Use Cases
* Great as a ResultSet cache. Have an expensive query that needs to run every time your app starts, and it's slowing down your development? Cache it locally on disk in a DataFrame! Because DataFrame can be viewed as a ResultSet you can plug it into existing code with minimal changes. Or cache in a service and pull it over the network (reads/writes directly to Channel).
* Great for generating Excel reports via POI - stage the data in a DataFrame first and then write POI code against the DataFrame. This separates concerns and is easier than writing POI directly against a ResultSet.

### Limitations
* Max \~2^31 rows (\~2.1 billion)
* Custom column types are not supported
* ResultSet view does not support ResultSetMetaData

### [Sample Usages](dataframe/dataframe-test/src/test/java/tech/bitey/dataframe/test/SampleUsages.java)

