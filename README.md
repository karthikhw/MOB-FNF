# MOB-FNF
Repro for MOB FileNotFound Issue

### 1. Setting Hfile Version
HFile version set to be 3 for MOB requirement.

Ambari -> HBase -> Configs -> Custom hbase-site -> hfile.format.version=3

### 2. Setting retry threshold
To repro reduce the retry count from 10 to 3 and split table from 1 region to 4 or above.
Ambari -> HBase -> Configs -> Custom hbase-site -> hbase.bulkload.retries.number=3

### 3. Restart HBase.

### 4. Create Table
$hbase shell
>create 'mobtest', {NAME => 'f1', IS_MOB => true, MOB_THRESHOLD => 10}

### 5. Create Data Generator code.

vi HBaseSamplePut.java

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseSamplePut {

  public static void main(String args[]) throws IOException {

    Configuration config = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(config);
    Table table = connection.getTable(TableName.valueOf(args[0]));
    Put put = null;
    for (int i = Integer.parseInt(args[1]); i < Integer.parseInt(args[2]); i++){
      put = new Put(Bytes.toBytes(String.valueOf(i)));
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("c1"), Bytes.toBytes("Data missing from MOB enabled CFs"));
      table.put(put);
    }
    table.close();
  }
}
````

### 6. Compile Data Generator Code

javac -cp \`hbase classpath\`: HBaseSamplePut.java


### 7. Generate data, range from key 1 to 1000

java -cp \`hbase classpath\`: HBaseSamplePut mobtest 1 1000

### 8. Flush Table

$hbase shell
> flush 'mobtest'

### 9. Generate data, range from key 1000 to 2000

java -cp \`hbase classpath\`: HBaseSamplePut mobtest 1000 2000

### 10. Flush Table

$hbase shell
> flush 'mobtest'

### 11. Split Region

 Find region name from HMaster UI and split into more than 3 regions.

Example:

hbase(main):015:0> split '52433cda879b874d09102140953b25a3'
0 row(s) in 0.0180 seconds

hbase(main):016:0> split 'f5f46c03393d36527be6cd2305636579'
0 row(s) in 0.0200 seconds

hbase(main):017:0> split '827147419fa8db42de15cb442983bce8'
0 row(s) in 0.0200 seconds

hbase(main):018:0> split '225dee5c61091722ef0cddc6bfd57e6c'
0 row(s) in 0.0170 seconds


### 11. Run MOB compaction
Issue can be reproduced with this compaction

$hbase shell
>compact 'mobtest','f1','MOB'
