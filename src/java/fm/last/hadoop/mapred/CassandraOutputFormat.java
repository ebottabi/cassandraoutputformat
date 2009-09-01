package fm.last.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import fm.last.hadoop.io.records.RowColumn;

public class CassandraOutputFormat implements OutputFormat<RowColumn, BytesWritable> {

  public static final String CONF_KEYSPACE = "cassandra.keyspace";
  public static final String CONF_COLUMN_FAMILY_NAME = "cassandra.cfname";

  @Override
  public void checkOutputSpecs(FileSystem fs, JobConf conf) throws IOException {
    if (null == conf.get(CONF_KEYSPACE)) {
      throw new IOException("Must specify the keyspace in the jobconf: " + CONF_KEYSPACE);
    }

    if (null == conf.get(CONF_COLUMN_FAMILY_NAME)) {
      throw new IOException("Must specify the column family name in the jobconf: " + CONF_COLUMN_FAMILY_NAME);
    }
  }

  @Override
  public RecordWriter<RowColumn, BytesWritable> getRecordWriter(FileSystem fs, JobConf conf, String name,
      Progressable reporter) throws IOException {

    return new CassandraRecordWriter(conf.get(CONF_KEYSPACE), conf.get(CONF_COLUMN_FAMILY_NAME), conf);
  }

  /**
   * This method tells each node to flush the newly transferred
   * data. You need to call this after the MapReduce job is done.
   * 
   * @param keyspace Cassandra keyspace
   * @param cfName Cassandra columnFamilyName
   * @param cassandraConfig Location of the storage-conf.xml
   * @throws IOException Failed to flush the nodes
   */
  public static void forceFlush(String keyspace, String cfName, String cassandraConfig) throws IOException {
    System.setProperty("storage-config", cassandraConfig);
    CassandraClient client = new CassandraClient(keyspace, cfName);
    client.forceFlush();
  }
  
}
