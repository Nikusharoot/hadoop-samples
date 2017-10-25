export HADOOP_CONF_DIR=/etc/hadoop/conf/


#spark-submit   --class com.sample.SimpleCountJob   --master yarn   --deploy-mode client   --executor-memory 512M  --num-executors 4   /home/cloudera/workspace/SparkSample/target/spark/SparkSample-0.0.1-SNAPSHOT-jar-with-dependencies.jar   10

#spark-submit   --class com.sample.Top10CountriesByNetMaskDFJob   --master yarn   --deploy-mode client   --executor-memory 512M  --num-executors 4   /home/cloudera/workspace/SparkSample/spark/target/SparkSample-0.0.1-SNAPSHOT-jar-with-dependencies.jar 

	spark-submit   --class com.sample.Top10CountriesNetmaskRDDJob   --master yarn   --deploy-mode client   --executor-memory 512M  --num-executors 4   /home/cloudera/workspace/SparkSample/spark/target/SparkSample-0.0.1-SNAPSHOT-jar-with-dependencies.jar 

#spark-submit   --class com.sample.TopMostFrequentlyDFJob   --master yarn   --deploy-mode client   --executor-memory 512M  --num-executors 4   /home/cloudera/workspace/SparkSample/spark/target/SparkSample-0.0.1-SNAPSHOT-jar-with-dependencies.jar 

#spark-submit   --class com.sample.TopMostFrequentlyRDD   --master yarn   --deploy-mode client   --executor-memory 512M  --num-executors 4   /home/cloudera/workspace/SparkSample/spark/target/SparkSample-0.0.1-SNAPSHOT-jar-with-dependencies.jar 
