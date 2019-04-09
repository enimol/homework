package com.howo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*; //HBaseConfiguration;
import org.apache.hadoop.hbase.client.*; //HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

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
				.add("department", "string")
				.add("designation", "string")
				.add("ctc", "long")
				.add("state", "string");

		Dataset<Row> df = spark.read()
				.option("mode", "DROPMALFORMED")
				.schema(schema)
				.csv("hdfs:///tmp/input.csv");
		//...
		System.out.println(df.count());
		Configuration config = null;
		try {
			config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "sandbox-hdp.hortonworks.com");
			config.set("zookeeper.znode.parent", "/hbase-unsecure");
			//config.set("hbase.zookeeper.property.clientPort","2181");
			//config.set("hbase.master", "127.0.0.1:60000");
			HBaseAdmin.available(config);
			System.out.println("HBase is running!");
			Connection connection = ConnectionFactory.createConnection(config);
			// Description of the declaration table
			TableName userTable  = TableName.valueOf("shb1");
			TableDescriptorBuilder tableDescr = TableDescriptorBuilder.newBuilder(userTable);
			tableDescr.setColumnFamily(ColumnFamilyDescriptorBuilder.of("info"));
			// Create a Table
			System.out.println("Creating table shb1. ");
			Admin admin = connection.getAdmin();
			if (admin.tableExists(userTable)) {
				admin.disableTable(userTable);
				admin.deleteTable(userTable);
			}
			admin.createTable(tableDescr.build());

			Table table = connection.getTable(TableName.valueOf("shb1"));
			List<Row> rowslist = df.collectAsList();
			Long rowNumber = 0L;
			byte[] familyName = Bytes.toBytes("info");
			for (Row row : rowslist) {
				rowNumber++;
				String[] columns = row.toString().split(",");
				Put put = new Put(Bytes.toBytes("row" + rowNumber));
		        put.addColumn(familyName, Bytes.toBytes("FirstName"), Bytes.toBytes(columns[0]));
		        put.addColumn(familyName, Bytes.toBytes("LastName"), Bytes.toBytes(columns[1]));
		        put.addColumn(familyName, Bytes.toBytes("Location"), Bytes.toBytes(columns[2]));
		        put.addColumn(familyName, Bytes.toBytes("Count"), Bytes.toBytes(columns[3]));
		        table.put(put);
			}
			
			connection.close();
		}catch (Exception ce){ 
			ce.printStackTrace();
		}
		context.close();
	}
}
