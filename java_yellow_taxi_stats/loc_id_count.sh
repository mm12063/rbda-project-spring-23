mvn package
hadoop fs -rm -r hdfs://nyu-dataproc-m/user/mm12063_nyu_edu/project/output_loc_id_count/
hadoop jar ~/rbda-project-spring-23/java_yellow_taxi_stats/target/YellowTaxiStats-1.0.jar YellowTaxiStats hdfs://nyu-dataproc-m/user/mm12063_nyu_edu/project/output_yellow_taxi_manhattan/ hdfs://nyu-dataproc-m/user/mm12063_nyu_edu/project/output_loc_id_count
hadoop fs -head hdfs://nyu-dataproc-m/user/mm12063_nyu_edu/project/output_loc_id_count/part-m-00000
