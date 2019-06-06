#### A note on lower case table/schema name
Phoenix table naming conventions are in uppercase. User may do mapping on existing HBase table into Phoenix with a lowercase table name. 
In this case, Use quotes in table name i.e "tablename" for bulkload.
Phoenix mainly uses Apache Commons CLI library for parsing command line option. 

Commons CLI has a problem in interpreting double quotes, Ref CLI-275. As a workaround, Use \\\"\\\"tablename\\\"\\\" will interpret to "tablename".

Example:

    hadoop jar phoenix-<version>-client.jar org.apache.phoenix.mapreduce.CsvBulkLoadTool --table \"\"t\"\" --input /data/example.csv
