package fm.last.hadoop.examples;

import org.apache.hadoop.util.ProgramDriver;
import org.apache.log4j.Logger;

public class Launcher {

  private static Logger log = Logger.getLogger(Launcher.class);
  
  public static void main(String argv[]) {
    try {
      ProgramDriver pgd = new ProgramDriver();
      pgd.addClass("batchimport", BatchImport.class, "Import already preprocessed data into Cassandra");
      pgd.driver(argv);
    } catch (Throwable e) {
      log.error("Could not start program", e);
    }
  }
}
