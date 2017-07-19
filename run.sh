for file in target/*.jar; do
export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:'$file';
done
hadoop jar target/emrfs-1.0.0.jar com.test.TestEmrfs
