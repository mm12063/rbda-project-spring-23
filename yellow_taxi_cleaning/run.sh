mvn package
hadoop fs -rm -r hdfs://nyu-dataproc-m/user/mm12063_nyu_edu/project/output_yellow_taxi_manhattan/
hadoop jar ~/rbda-project-spring-23/java_yellow_taxi_cleaning/target/YellowTaxi-1.0.jar YellowTaxi hdfs://nyu-dataproc-m/user/mm12063_nyu_edu/project/taxi_zones.csv hdfs://nyu-dataproc-m/user/mm12063_nyu_edu/project/parquet_files hdfs://nyu-dataproc-m/user/mm12063_nyu_edu/project/output_yellow_taxi_manhattan
hadoop fs -head hdfs://nyu-dataproc-m/user/mm12063_nyu_edu/project/output_yellow_taxi_manhattan/part-m-00000

