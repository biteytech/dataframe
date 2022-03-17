# dataframe

[![Apache 2.0](https://img.shields.io/github/license/nebula-plugins/nebula-project-plugin.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Build Status](https://api.travis-ci.com/biteytech/dataframe.svg?branch=master)](https://app.travis-ci.com/github/biteytech/dataframe)

Another dataframe library for Java, inspired by Tablesaw, built on nio buffers.

To add a dependency on dataframe using Maven, use the following:

```xml
<dependency>
  <groupId>tech.bitey</groupId>
  <artifactId>dataframe</artifactId>
  <version>1.2.1</version>
</dependency>
```

Requires Java 17 or higher. The last version supporting Java 11 was `1.1.7`.

### [Sample Usages](dataframe/dataframe-test/src/test/java/tech/bitey/dataframe/test/SampleUsages.java)

### [Release Notes](https://github.com/biteytech/dataframe/wiki#release-notes)

### What's different about this dataframe library?
* It's geared towards making it easier to ship around tabular data for Java backend developers - rather than for data science. This is not Pandas for Java.
* Data is stored in ByteBuffers, so the dataframes can read/write to Channels with minimal overhead (save to files, send over network).
* Optimized for space. For example, booleans take one bit each, DateTimes take one long (with microsecond precision).
* Nulls are stored in a separate bitset (also backed by ByteBuffer), taking up two bits per Column length. No extra space is used if all values are non-null.

### Features
* Supports the most common types: String, Integer, Long, Short, Byte, Boolean, Double, Float, Date, DateTime, and BigDecimal; as well as Time, UUID, and Instant.
* Column and DataFrame are immutable. Columns can be created from collections, arrays, streams, or with builders.
* Read/write to File or Channel with minimal overhead
* Read/write CSV files
* Read from ResultSet, write with PreparedStatement
* Backing ByteBuffers can be on heap or off as a global property: `-Dtech.bitey.allocateDirect=true` or `false`, defaults to `false`
* Column implements List. DataFrame implements `List<Row>`. If the DataFrame has a key column it can be viewed as a `NavigableMap<T, Row>`
* Basic filtering, joining, grouping
* No additional dependencies
* Extensive unit testing

### Sample Use Cases
* Great as a ResultSet cache. Have an expensive query that needs to run every time your app starts and it's slowing down your development? Cache it locally on disk in a DataFrame! Because DataFrame can be viewed as a ResultSet, you can plug it into existing code with minimal changes. Or cache it in a service and pull it over the network (reads/writes directly to Channel).
* Great for generating Excel reports via POI. Stage the data in a DataFrame first, then write POI code against the DataFrame. This separates concerns and is easier than writing POI directly against a ResultSet.

