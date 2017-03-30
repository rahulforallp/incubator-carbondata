package org.apache.carbondata.examples;

/**
 * Created by rahul on 28/3/17.
 */
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class LibsUtils {

  // Logger
  private static final Logger LOG = LoggerFactory.getLogger(LibsUtils.class);

  public static void setHadoopHome() {

    // Set hadoop.home.dir to point to the linux lib dir
    if (System.getProperty("os.name").toLowerCase().startsWith("linux")) {

      String libDir = getHadoopHome();

      LOG.info(" starting hadoop_home : {}", libDir);

      ProcessBuilder pb = new ProcessBuilder(libDir+"/sbin/start-all.sh");
      Process p = null;
      try {
        p = pb.start();
      } catch (IOException e) {
        e.printStackTrace();
      }
      BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String line = null;
      try {
        while ((line = reader.readLine()) != null)
        {
          System.out.println(line);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }


     /* System.setProperty("hadoop.home.dir", libDir);
      System.load(new File(libDir + Path.SEPARATOR + "lib" + Path.SEPARATOR + "hadoop.dll").getAbsolutePath());
      System.load(new File(libDir + Path.SEPARATOR + "lib" + Path.SEPARATOR + "hdfs.dll").getAbsolutePath());
*/
    }
  }

  public static String getHadoopHome() {

    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(new File(new File(System.getProperty("user.home")), ".bashrc")));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    try {
      for(String line = reader.readLine(); line != null; line = reader.readLine()) {
      if (line.startsWith("$HADOOP_HOME") || line.startsWith("export HADOOP_HOME")) {
        return line.split("=")[1];
      }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return "nohadoophomefound";
    //operations
  }


}
