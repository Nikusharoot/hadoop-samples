export HADOOP_CONF_DIR=/etc/hadoop/conf/


spark-submit   --class com.sample.SimpleCountJob   --master yarn   --deploy-mode client   --executor-memory 512M  --num-executors 4   /home/cloudera/workspace/SparkSample/target/SparkSample-0.0.1-SNAPSHOT-jar-with-dependencies.jar   10
