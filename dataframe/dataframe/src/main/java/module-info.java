module tech.bitey.dataframe {

	exports tech.bitey.dataframe;
	exports tech.bitey.dataframe.db;

	requires tech.bitey.bufferstuff;
	requires transitive java.sql;
	requires org.joda.beans;
}
