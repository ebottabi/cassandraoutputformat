package fm.last.hadoop.labs.cassandra;

import java.io.IOException;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import fm.last.hadoop.io.records.RowColumn;

public class CassandraRecordWriter implements RecordWriter<RowColumn, BytesWritable> {

  private String keyspace;
  private String cfName;
  private CassandraClient client;
  
  private ColumnFamily columnFamily;

  private String lastRowKey;

  /**
   * @param keyspace Keyspace to write to
   * @param cfName Column family to write to
   * @param conf JobConf of the job
   */
  public CassandraRecordWriter(String keyspace, String cfName, JobConf conf) {
    this.keyspace = keyspace;
    this.cfName = cfName;
    this.client = new CassandraClient(keyspace, cfName);
    configure(conf);
  }

  @Override
  public void write(RowColumn key, BytesWritable value) throws IOException {
    if (!key.getRowKey().equals(lastRowKey)) {
      if (lastRowKey != null) {
        // send off the gathered data, but not if we haven't gathered any yet
        client.sendColumnFamily(key.getRowKey(), columnFamily);
      }

      // new row key, let's clean up
      this.columnFamily = ColumnFamily.create(keyspace, cfName);
    }

    // set to null if it's empty
    byte[] superCol = key.getSuperColumnName().getCount() == 0 ? null : key.getSuperColumnName().get();

    // behaviour copied from CassandraBulkLoader
    // keep adding upp the ones with the same rowkey and send off together
    columnFamily.addColumn(new QueryPath(cfName, superCol, key.getColumnName().get()), value.get(), 0);

    this.lastRowKey = key.getRowKey();
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    if (lastRowKey != null) {
      client.sendColumnFamily(lastRowKey, columnFamily);
    }

    client.shutdownMessagingService();
  }

  /**
   * Set up the cluster ring, start messaging service
   * 
   * @param job Configuration
   */
  protected void configure(JobConf job) {
    try {
      // Get the cached files
      // TODO don't assume it's the first file
      Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
      String cassConfig = localFiles[0].getParent().toString();
      System.setProperty("storage-config", cassConfig);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    client.startMessagingService();

    try {
      client.updateTokenMetadata();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


}