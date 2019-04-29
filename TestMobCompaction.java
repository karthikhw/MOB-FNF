package org.apache.hadoop.hbase.mob.compactions;
import java.lang.Thread;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin.CompactType;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Arrays;

@Category(MediumTests.class)
public class TestMobCompaction {
    private static final Log LOG = LogFactory.getLog(TestMobCompaction.class);

    private static final HBaseTestingUtility HTU = new HBaseTestingUtility();

    private final static String famStr = "f1";
    private final byte[] fam = Bytes.toBytes(famStr);
    private final byte[] qualifier = Bytes.toBytes("q1");
    private final long mobLen = 10;
    private byte[] mobVal = Bytes.toBytes("01234567890");


    @BeforeClass
    public static void beforeClass() throws Exception {
        HTU.getConfiguration().setInt("hfile.format.version", 3);
        HTU.getConfiguration().setLong(TimeToLiveHFileCleaner.TTL_CONF_KEY, 0);
        HTU.getConfiguration().setInt("hbase.client.retries.number", 1);
        HTU.getConfiguration().setInt("hbase.hfile.compaction.discharger.interval", 100);
        HTU.startMiniCluster();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        HTU.shutdownMiniCluster();
    }


    @Test
    public void testMobCompaction() throws InterruptedException, IOException {
        // Create table then get the single region for our new table.
        HTableDescriptor hdt = HTU.createTableDescriptor("testMobCompactTable");
        HColumnDescriptor hcd= new HColumnDescriptor(fam);

        hcd.setMobEnabled(true);
        hcd.setMobThreshold(mobLen);
        hcd.setMaxVersions(1);
        hdt.addFamily(hcd);

        try {
            Table table = HTU.createTable(hdt, null);

            HRegion r = HTU.getMiniHBaseCluster().getRegions(hdt.getTableName()).get(0);


            int count = 100;
            //Put Operation
            for (int i = 0; i < count; i++) {

                Put p = new Put(Bytes.toBytes("r"+i));
                p.addColumn(fam, qualifier, mobVal);
                table.put(p);
                HTU.flush(table.getName());

                p = new Put(Bytes.toBytes("r"+i+1));
                p.addColumn(fam, qualifier, mobVal);
                table.put(p);
                HTU.flush(table.getName());

                p = new Put(Bytes.toBytes("r"+i+2));
                p.addColumn(fam, qualifier, mobVal);
                table.put(p);
                HTU.flush(table.getName());

                p = new Put(Bytes.toBytes("r"+i+3));
                p.addColumn(fam, qualifier, mobVal);
                table.put(p);
                HTU.flush(table.getName());


                p = new Put(Bytes.toBytes("r"+i+4));
                p.addColumn(fam, qualifier, mobVal);
                table.put(p);
                HTU.flush(table.getName());


                p = new Put(Bytes.toBytes("r"+i+5));
                p.addColumn(fam, qualifier, mobVal);
                table.put(p);
                HTU.flush(table.getName());

                p = new Put(Bytes.toBytes("r"+i+6));
                p.addColumn(fam, qualifier, mobVal);
                table.put(p);
                HTU.flush(table.getName());


                p = new Put(Bytes.toBytes("r"+i+7));
                p.addColumn(fam, qualifier, mobVal);
                table.put(p);
                HTU.flush(table.getName());


                //Minor Compaction
                //HTU.getHBaseAdmin().compact(hdt.getTableName(), fam);
                //Major Compaction
                HTU.getHBaseAdmin().majorCompact(hdt.getTableName(), fam);
                //MOB Compaction
                HTU.getHBaseAdmin().compact(hdt.getTableName(), fam, CompactType.MOB);
                Thread.sleep(100);
                //Clean Archive
                HTU.getMiniHBaseCluster().getMaster().getHFileCleaner().choreForTesting();
            }
            Thread.sleep(10000);
            try {
                Get get;Result result;
                for (int i = 0; i < count ; i++) {
                     get = new Get(Bytes.toBytes("r"+i));
                     result = table.get(get);
                     assertTrue(Arrays.equals(result.getValue(fam, qualifier), mobVal));
                }
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
        } finally {

            HTU.getHBaseAdmin().disableTable(hdt.getTableName());
            HTU.deleteTable(hdt.getTableName());
        }
    }
}
