hdfs dfs -rm $2/*
hdfs dfs -rmdir $2
hadoop jar ../MapReduce/homework-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.howo.HomeWork $1 $2
