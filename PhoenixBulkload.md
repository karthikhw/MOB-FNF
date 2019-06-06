#### A note on lower case table/schema name
Table names in Phoenix are case insensitive( generally uppercase). but sometimes user may require to do mapping of existing HBase table with lowercase name into Phoenix table, In this case, Double quotes around table name i.e "tablename" can be used to preserve case sensitivity. The same was extended to the bulkload options, but due to the way Apache Commons CLI library parse command line options(Ref CLI-275), we need to pass the argument as \"\"tablename\"\" instead of just "tablename" for CsvBulkLoadTool.

Example:

    hadoop jar phoenix-<version>-client.jar org.apache.phoenix.mapreduce.CsvBulkLoadTool --table \"\"t\"\" --input /data/example.csv
