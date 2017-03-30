package org.apache.carbondata.examples;

/**
 * Created by rahul on 28/3/17.
 */
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.apache.carbondata.examples.HiveLocalMetaStore.buildHiveConf;

public class HiveLocalServer2  {

  // Logger
  private static final Logger LOG = LoggerFactory.getLogger(HiveLocalServer2.class);

  private HiveServer2 hiveServer2;

  private String hiveServer2Hostname;
  private Integer hiveServer2Port;
  private String hiveMetastoreHostname;
  private Integer hiveMetastorePort;
  private String hiveMetastoreDerbyDbDir;
  private String hiveScratchDir;
  private String hiveWarehouseDir;
  private HiveConf hiveConf;
  private String zookeeperConnectionString;

  public String getHiveServer2Hostname() {
    return hiveServer2Hostname;
  }

  public Integer getHiveServer2Port() {
    return hiveServer2Port;
  }

  public String getHiveMetastoreHostname() {
    return hiveMetastoreHostname;
  }

  public Integer getHiveMetastorePort() {
    return hiveMetastorePort;
  }

  public String getHiveMetastoreDerbyDbDir() {
    return hiveMetastoreDerbyDbDir;
  }

  public String getHiveScratchDir() {
    return hiveScratchDir;
  }

  public String getHiveWarehouseDir() {
    return hiveWarehouseDir;
  }

  public HiveConf getHiveConf() {
    return hiveConf;
  }

  public String getZookeeperConnectionString() {
    return zookeeperConnectionString;
  }

  private HiveLocalServer2(Builder builder) {
    this.hiveServer2Hostname = builder.hiveServer2Hostname;
    this.hiveServer2Port = builder.hiveServer2Port;
    this.hiveMetastoreHostname = builder.hiveMetastoreHostname;
    this.hiveMetastorePort = builder.hiveMetastorePort;
    this.hiveMetastoreDerbyDbDir = builder.hiveMetastoreDerbyDbDir;
    this.hiveScratchDir = builder.hiveScratchDir;
    this.hiveWarehouseDir = builder.hiveWarehouseDir;
    this.hiveConf = builder.hiveConf;
    this.zookeeperConnectionString = builder.zookeeperConnectionString;
  }

  public static class Builder {

    private String hiveServer2Hostname;
    private Integer hiveServer2Port;
    private String hiveMetastoreHostname;
    private Integer hiveMetastorePort;
    private String hiveMetastoreDerbyDbDir;
    private String hiveScratchDir;
    private String hiveWarehouseDir;
    private HiveConf hiveConf;
    private String zookeeperConnectionString;

    public Builder setHiveServer2Hostname(String hiveServer2Hostname) {
      this.hiveServer2Hostname = hiveServer2Hostname;
      return this;
    }

    public Builder setHiveServer2Port(Integer hiveServer2Port) {
      this.hiveServer2Port = hiveServer2Port;
      return this;
    }

    public Builder setHiveMetastoreHostname(String hiveMetastoreHostname) {
      this.hiveMetastoreHostname = hiveMetastoreHostname;
      return this;
    }

    public Builder setHiveMetastorePort(Integer hiveMetastorePort) {
      this.hiveMetastorePort = hiveMetastorePort;
      return this;
    }

    public Builder setHiveMetastoreDerbyDbDir(String hiveMetastoreDerbyDbDir) {
      this.hiveMetastoreDerbyDbDir = hiveMetastoreDerbyDbDir;
      return this;
    }

    public Builder setHiveScratchDir(String hiveScratchDir) {
      this.hiveScratchDir = hiveScratchDir;
      return this;
    }

    public Builder setHiveWarehouseDir(String hiveWarehouseDir) {
      this.hiveWarehouseDir = hiveWarehouseDir;
      return this;
    }

    public Builder setHiveConf(HiveConf hiveConf) {
      this.hiveConf = hiveConf;
      return this;
    }

    public Builder setZookeeperConnectionString(String zookeeperConnectionString) {
      this.zookeeperConnectionString = zookeeperConnectionString;
      return this;
    }

    public HiveLocalServer2 build() {
      HiveLocalServer2 hiveLocalServer2 = new HiveLocalServer2(this);
      validateObject(hiveLocalServer2);
      return hiveLocalServer2;
    }

    public void validateObject(HiveLocalServer2 hiveLocalServer2) {
      if(hiveLocalServer2.hiveServer2Hostname == null) {
        throw new IllegalArgumentException("ERROR: Missing required config: Hive Server2 Hostname");
      }

      if(hiveLocalServer2.hiveServer2Port == null) {
        throw new IllegalArgumentException("ERROR: Missing required config: Hive Server2 Port");
      }

      if(hiveLocalServer2.hiveMetastoreHostname == null) {
        throw new IllegalArgumentException("ERROR: Missing required config: Hive Meta Store Hostname");
      }

      if(hiveLocalServer2.hiveMetastorePort == null) {
        throw new IllegalArgumentException("ERROR: Missing required config: Hive Meta Store Port");
      }

      if(hiveLocalServer2.hiveMetastoreDerbyDbDir == null) {
        throw new IllegalArgumentException("ERROR: Missing required config: Hive Meta Store Derby Db Dir");
      }

      if(hiveLocalServer2.hiveScratchDir == null) {
        throw new IllegalArgumentException("ERROR: Missing required config: Hive Scratch Dir");
      }

      if(hiveLocalServer2.hiveWarehouseDir == null) {
        throw new IllegalArgumentException("ERROR: Missing required config: Hive Warehouse Dir");
      }

      if(hiveLocalServer2.hiveConf == null) {
        throw new IllegalArgumentException("ERROR: Missing required config: Hive Conf");
      }


    }

  }



  public void start() throws Exception {
    hiveServer2 = new HiveServer2();
    LOG.info("HIVESERVER2: Starting HiveServer2 on port: {}", hiveServer2Port);
    configure();
    hiveServer2.init(hiveConf);
    hiveServer2.start();
  }


  public void stop() throws Exception {
    stop(true);
  }


  public void stop(boolean cleanUp) throws Exception {
    LOG.info("HIVESERVER2: Stopping HiveServer2 on port: {}", hiveServer2Port);
    hiveServer2.stop();
    if (cleanUp) {
      cleanUp();
    }
  }


  public void configure() throws Exception {

    // Handle Windows
    LibsUtils.setHadoopHome();

    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS,
        "thrift://" + hiveMetastoreHostname + ":" + hiveMetastorePort);
    hiveConf.setVar(HiveConf.ConfVars.SCRATCHDIR, hiveScratchDir);
    hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY,
        "jdbc:derby:;databaseName=" + hiveMetastoreDerbyDbDir + ";create=true");
    hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, new File(hiveWarehouseDir).getAbsolutePath());
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST, true);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, String.valueOf(hiveServer2Hostname));
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, hiveServer2Port);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM, zookeeperConnectionString);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, Boolean.TRUE);
  }


  public void cleanUp() throws Exception {
    FileUtils.deleteFolder(hiveMetastoreDerbyDbDir);
    FileUtils.deleteFolder(hiveScratchDir);
    FileUtils.deleteFolder(new File("derby.log").getAbsolutePath());
  }

  public static HiveLocalServer2 setup(String currentPath){
   HiveLocalServer2 hiveLocalServer2 = new Builder()
        .setHiveServer2Hostname("localhost")
        .setHiveServer2Port(10001)
        .setHiveMetastoreHostname("localhost")
        .setHiveMetastorePort(10011)
        .setHiveMetastoreDerbyDbDir(currentPath+"/target")
        .setHiveScratchDir(currentPath+"/target")
        .setHiveWarehouseDir(currentPath+"/target")
        .setHiveConf(buildHiveConf())
        .build();
    try
    {
      hiveLocalServer2.start();
    }
    catch (Exception x){
      System.out.println("===================starting hivesrver=======================");
    }
    return hiveLocalServer2;
  }

}