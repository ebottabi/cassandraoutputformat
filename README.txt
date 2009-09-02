Cassandra output format for Hadoop 0.18

Written by Johan Oskarsson, heavily based on code and research by 
Chris Goffinet from Digg:
http://github.com/lenn0x/Cassandra-Hadoop-BMT/tree/master

What does it do?
----------------
The CassandraOutputFormat is a way to insert data into Cassandra from a
Hadoop MapReduce program at high throughput. It is using the Cassandra
BinaryMemtable to achieve high speeds without the overhead of the 
traditional insertion methods.


How to use
----------
Setup your jobconf with these values (and whatever else your program needs):

conf.setOutputKeyClass(RowColumn.class);
conf.setOutputValueClass(BytesWritable.class);

conf.setOutputFormat(CassandraOutputFormat.class);
conf.set(CassandraOutputFormat.CONF_COLUMN_FAMILY_NAME, "columnfamilyname");
conf.set(CassandraOutputFormat.CONF_KEYSPACE, "keyspacename");

// We need your storage-conf.xml from the Cassandra cluster
DistributedCache.addCacheFile(new URI("uri_to_storage-conf.xml"), conf);

Then run you job. Afterwards you need to call CassandraOutputFormat.forceFlush
to make sure the data is properly flushed.


Notes
-----
* Writing to supercolumns is not tested
* The jar created by the build file is to be used on a Hadoop cluster and as such
  it contains all the jars in lib. If you will include it in your program then
  don't include the jars in the cassandraoutputformat.jar but in your file instead.

Troubleshooting
---------------
* Make sure you have the same cassandra.jar in the Hadoop program as on the 
  Cassandra cluster to avoid strange RMI errors.
* Make sure no ghost processes are left behind, sometimes the Cassandra daemons
  don't shut down properly
* If you run into out of memory errors you can tweak BinaryMemtableSizeInMB and
  FlushMaxThreads on the Cassandra cluster. You can also reduce the number 
  of reducers on the Hadoop side.