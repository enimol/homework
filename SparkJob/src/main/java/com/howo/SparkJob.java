package com.howo;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author Imre Molnar
 *
 */
public final class SparkJob {

  /**
   * For the maven check-style compliance.
   *
   */
  private SparkJob() {
  }

  /**
   * This is my job.
   * @param args  args[0]: folder of input file(s), args[1]: table name.
   *
   */
  public static void main(final String[] args) {

    SparkConf sparkConf = new SparkConf();
    sparkConf.setAppName("Hello Spark");
    sparkConf.setMaster("local");

    JavaSparkContext context = new JavaSparkContext(sparkConf);

    String inputPath = new String(args[0] + "/part-r-*");

    SparkSession spark = SparkSession
        .builder()
        .appName("Java Spark SQL Example")
        .getOrCreate();

    StructType schema = new StructType()
        .add("FirstName", "string")
        .add("LastName", "string")
        .add("Location", "string")
        .add("BirthDate", "string");

    Dataset<Row> df = spark.read()
        .option("mode", "DROPMALFORMED")
        .option("header", "false")
        .schema(schema)
        .csv(inputPath);

    // check the number of rows
    System.out.println("Number of rows in csv:" + df.count());
    Configuration config = null;
    try {
      config = HBaseConfiguration.create();
      config.set("hbase.zookeeper.quorum", "sandbox-hdp.hortonworks.com");
      config.set("zookeeper.znode.parent", "/hbase-unsecure");
      HBaseAdmin.available(config);
      System.out.println("HBase is running!");
      Connection connection = ConnectionFactory.createConnection(config);
      // Description of the declaration table
      TableName userTable  = TableName.valueOf(args[1]);
      TableDescriptorBuilder tableDescr =
          TableDescriptorBuilder.newBuilder(userTable);
      tableDescr.setColumnFamily(ColumnFamilyDescriptorBuilder.of("info"));
      // Create a Table
      System.out.println("Creating table " + args[1]);
      Admin admin = connection.getAdmin();
      if (admin.tableExists(userTable)) {
        admin.disableTable(userTable);
        admin.deleteTable(userTable);
      }
      admin.createTable(tableDescr.build());

      Table table = connection.getTable(TableName.valueOf(args[1]));
      System.out.println("TABLE CREATED: " + table.getName().toString());
      List<Row> rowslist = df.collectAsList();
      System.out.println("Collected rows from csv:" + rowslist.size());
      Long rowNumber = 0L;
      byte[] familyName = Bytes.toBytes("info");
      for (Row row : rowslist) {
        Put put = new Put(Bytes.toBytes("row" + rowNumber));
        put.addColumn(familyName,
            Bytes.toBytes("FirstName"),
            Bytes.toBytes(row.getString(0)));
        put.addColumn(familyName,
            Bytes.toBytes("LastName"),
            Bytes.toBytes(row.getString(1)));
        put.addColumn(familyName,
            Bytes.toBytes("Location"),
            Bytes.toBytes(row.getString(2)));
        put.addColumn(familyName,
            Bytes.toBytes("Count"),
            Bytes.toBytes(((Long) df.count()).toString()));
        table.put(put);
        rowNumber++;
        System.out.println("Stored line:" + rowNumber);
      }

      connection.close();
    } catch (Exception ce) {
      ce.printStackTrace();
    }
    context.close();
  }
}
