# OutputFilesListener

A Spark listener used for metrics calculation of output files, like output bytes/records count and files number.

Spark edition is 1.6.1 in use. After testing in the cluster, I found that the metrics statistics in spark event is not exactly correct. For Spark SQL create_as_select/insert statements, output info are not collected even though the table data have been handled and copy to the HDFS path. For normal Spark shell command, like saveAsTextFile, metrics statistics work fine but the first task in each executor (mark output bytes as 0, but output records is correct), no matter in Spark 1.6.1 or 2.2.0.

This OutputFilesListener does not work very well because of the dependency to Spark event realization. Maybe we have to debug Spark first.

# shell arguments

`spark-shell --conf spark.extraListeners=com.paic.spark.OutputFilesListener --conf spark.driver.extraClassPath=/home/hadoop/spark/lib/OutputFilesMonitor-1.0.0.jar`
