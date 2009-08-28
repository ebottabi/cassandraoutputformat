package fm.last.hadoop.labs.cassandra;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.SelectorManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.log4j.Logger;

/**
 * Contains the methods that do the actuall communication
 * and management of the Cassandra cluster in order to do
 * the bulk loads.
 */
public class CassandraClient {

  private static Logger log = Logger.getLogger(CassandraClient.class);

  private String keyspace;
  private String cfName;

  public CassandraClient(String keyspace, String cfName) {
    this.keyspace = keyspace;
    this.cfName = cfName;
  }

  /**
   * Make the message and send to all the right nodes
   * 
   * @param rowKey Row key of the data
   * @param columnFamily Column family with data to send
   */
  public void sendColumnFamily(String rowKey, ColumnFamily columnFamily) {
    List<ColumnFamily> columnFamilies = new LinkedList<ColumnFamily>();
    columnFamilies.add(columnFamily);

    /* Get serialized message to send to cluster */
    Message message = createMessage(keyspace, rowKey, cfName, columnFamilies);
    for (EndPoint endpoint : StorageService.instance().getNStorageEndPoint(rowKey)) {
      /* Send message to end point */
      MessagingService.getMessagingInstance().sendOneWay(message, endpoint);
    }
  }

  /**
   * Create a message to send to the Cassandra nodes
   * 
   * @param keyspace Keyspace we are writing to
   * @param key Row key to write
   * @param cfName Column family name
   * @param columnFamiles Columns to write to the column family
   * @return The message to send
   */
  protected Message createMessage(String keyspace, String key, String cfName, List<ColumnFamily> columnFamiles) {
    ColumnFamily baseColumnFamily;
    DataOutputBuffer bufOut = new org.apache.cassandra.io.DataOutputBuffer();
    RowMutation rm;
    Message message;
    Column column;

    /* Get the first column family from list, this is just to get past validation */
    baseColumnFamily = new ColumnFamily(cfName, "Standard", DatabaseDescriptor.getComparator(keyspace, cfName),
        DatabaseDescriptor.getSubComparator(keyspace, cfName));

    for (ColumnFamily cf : columnFamiles) {
      bufOut.reset();
      try {
        ColumnFamily.serializer().serializeWithIndexes(cf, bufOut);
        byte[] data = new byte[bufOut.getLength()];
        System.arraycopy(bufOut.getData(), 0, data, 0, bufOut.getLength());

        column = new Column(cf.name().getBytes("UTF-8"), data, 0, false);
        baseColumnFamily.addColumn(column);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    rm = new RowMutation(keyspace, StorageService.getPartitioner().decorateKey(key));
    rm.add(baseColumnFamily);

    try {
      /* Make message */
      message = rm.makeRowMutationMessage(StorageService.binaryVerbHandler_);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return message;
  }

  /**
   * Start the Cassandra messaging service
   */
  public void startMessagingService() {
    SelectorManager.getSelectorManager().start();
  }

  /**
   * Shut down the Cassandra messaging service
   */
  public void shutdownMessagingService() {
    try {
      // Sleep just in case the number of keys we send over is small
      Thread.sleep(3 * 1000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    // Not implemented in Cassandra trunk, patch forth coming
    MessagingService.flushAndshutdown();
  }

  /**
   * Find out the token for each endpoint and set up the storage service accordingly.
   * 
   * @throws IOException could not update token metadata
   */
  protected void updateTokenMetadata() throws IOException {
    NodeProbe nodeProbe = getNodeProbe();
    Map<Range, List<EndPoint>> rangeToEndPoint = nodeProbe.getRangeToEndpointMap();

    for (Map.Entry<Range, List<EndPoint>> entry : rangeToEndPoint.entrySet()) {
      @SuppressWarnings("unchecked")
      Token token = entry.getKey().left();
      EndPoint ep = entry.getValue().get(0);
      log.debug("Setting up token metadata: " + token + " " + ep);
      StorageService.instance().updateTokenMetadata(token, ep);
    }
  }

  /**
   * Get the nodeprobe to one of the seed hosts TODO shouldn't really be static, perhaps move all the cassandra code
   * into
   * 
   * @return A nodeprobe instance
   * @throws IOException We couldn't connect to any of the nodes
   */
  protected NodeProbe getNodeProbe() throws IOException {
    Set<String> seedHosts = DatabaseDescriptor.getSeeds();
    for (String host : seedHosts) {
      try {
        // using default port
        return new NodeProbe(host);
      } catch (IOException e) {
        log.warn("Could not connect to seed: " + host, e);
      }
    }

    throw new IOException("Could not connect to any of the seeds");
  }

  /**
   * Force the binary memtables to flush
   * 
   * @throws IOException
   */
  public void forceFlush() throws IOException {
    NodeProbe nodeProbe = getNodeProbe();
    Set<EndPoint> allEps = new HashSet<EndPoint>();

    for (List<EndPoint> eps : nodeProbe.getRangeToEndpointMap().values()) {
      allEps.addAll(eps);
    }

    for (EndPoint endPoint : allEps) {
      try {
        NodeProbe np = new NodeProbe(endPoint.getHost());
        np.forceTableFlushBinary(keyspace);
        log.debug("Flushed: " + endPoint.getHost() + " " + keyspace);
      } catch (IOException e) {
        log.warn("Failed to flush: " + endPoint.getHost(), e);
      }
    }
  }

}
