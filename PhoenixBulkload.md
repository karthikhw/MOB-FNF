#### A note on lower case table/schema name
Phoenix table naming converstions are in uppercase. User may do mapping on existing hbase table into phoenix with a lowercase table name. 
In this case, Use quotes in tablename i.e "tablename" for bulkload.
Phoenix mainly use Apache Commons CLI library for parsing command line option. 

Commons CLI has a problem in interpreting double quotes, Ref CLI-275. As a workaround, Use \\\"\\\"\<tablename\>\\\"\\\" will interprete to "tablename".

Example:

    hadoop jar phoenix-<version>-client.jar org.apache.phoenix.mapreduce.CsvBulkLoadTool --table \"\"t\"\" --input /data/example.csv
