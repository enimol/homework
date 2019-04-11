package com.howo;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*; //HBaseConfiguration;
import org.apache.hadoop.hbase.client.*; //HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class SparkJob {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Hello Spark");
        sparkConf.setMaster("local");

        JavaSparkContext context = new JavaSparkContext(sparkConf);

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
                .option("header","false")
                .schema(schema)
                .csv(args[0]); // for example: "hdfs:///tmp/output/part-r-00000");

        // check the number of rows
        System.out.println(df.count());
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
            TableDescriptorBuilder tableDescr = TableDescriptorBuilder.newBuilder(userTable);
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
            List<Row> rowslist = df.collectAsList();
            Long rowNumber = 0L;
            byte[] familyName = Bytes.toBytes("info");
            for (Row row : rowslist) {
                Put put = new Put(Bytes.toBytes("row" + rowNumber));
                put.addColumn(familyName, Bytes.toBytes("FirstName"), Bytes.toBytes(row.getString(0)));
                put.addColumn(familyName, Bytes.toBytes("LastName"), Bytes.toBytes(row.getString(1)));
                put.addColumn(familyName, Bytes.toBytes("Location"), Bytes.toBytes(row.getString(2)));
                put.addColumn(familyName, Bytes.toBytes("Count"), Bytes.toBytes(((Long) df.count()).toString()));
                table.put(put);
                rowNumber++;
            }

            connection.close();
        } catch (Exception ce) {
            ce.printStackTrace();
        }
        context.close();
    }
}