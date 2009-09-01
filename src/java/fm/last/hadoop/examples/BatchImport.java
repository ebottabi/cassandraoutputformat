package fm.last.hadoop.examples;

import java.io.IOException;
import java.net.URI;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import fm.last.hadoop.io.records.RowColumn;
import fm.last.hadoop.mapred.CassandraOutputFormat;

public class BatchImport extends Configured implements Tool {

  /**
   * The input line should be in format: [rowkey][tab][columnname][tab][value][\n]
   */
  public static class BatchMapper extends MapReduceBase implements Mapper<LongWritable, Text, RowColumn, BytesWritable> {

    private Pattern split = Pattern.compile("\t");

    @Override
    public void map(LongWritable pos, Text line, OutputCollector<RowColumn, BytesWritable> output, Reporter reporter)
      throws IOException {

      String[] parts = split.split(line.toString());
      if(parts.length < 3) {
        reporter.incrCounter("error", "invalidline", 1);
        return;
      }
      
      output.collect(new RowColumn(parts[0], new Buffer(), new Buffer(parts[1].getBytes("UTF-8"))), new BytesWritable(
          parts[2].getBytes("UTF-8")));
    }

  }

  /**
   * Run an import into Cassandra
   * 
   * @param inputDir Input directory
   * @param storageConf storage-conf.xml from your Cassandra cluster
   * @param keyspace Keyspace to import to
   * @param cfName Column family name
   * @throws IOException 
   */
  public void start(Path inputDir, URI storageConf, String keyspace, String cfName) throws IOException {
    JobConf conf = new JobConf(getConf(), BatchImport.class);

    DistributedCache.addCacheFile(storageConf, conf);

    conf.setOutputKeyClass(RowColumn.class);
    conf.setOutputValueClass(BytesWritable.class);

    conf.setOutputFormat(CassandraOutputFormat.class);
    conf.set(CassandraOutputFormat.CONF_COLUMN_FAMILY_NAME, cfName);
    conf.set(CassandraOutputFormat.CONF_KEYSPACE, keyspace);

    conf.setInputFormat(TextInputFormat.class);
    conf.setJobName("cassandrabatchimport" + inputDir);

    conf.setMapperClass(BatchMapper.class);

    FileInputFormat.setInputPaths(conf, inputDir);

    JobClient.runJob(conf);

    // then flush it
    CassandraOutputFormat.forceFlush(keyspace, cfName, storageConf.toString());
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 4) {
      System.out.println("args <inputdir> <storageconfuri> <keyspace> <columnfamily>");
      return -1;
    }

    Path inputDir = new Path(args[0]);
    URI storageConf = new URI(args[1]);
    String keyspace = args[2];
    String cfName = args[3];

    start(inputDir, storageConf, keyspace, cfName);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BatchImport(), args);
  }
  
}
