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

/**
Repro for MOB datalosss Scenario

1. Insert 10 Million records
 2. In the mean time
      a) Trigger MOB Compaction (every 2 minutes)
      b) Trigger normal major compaction (every 2 minutes)
      c) Trigger archive cleaner (every 3 minutes)
 3. Minor compaction frequently triggered because the size of the flush is small
      a) Region Size 10 MB
      b) Flush size 1 MB
 4. Data validation started after 1 hour
 */
@Category(MediumTests.class)
public class TestMobCompaction {
    private static final Log LOG = LogFactory.getLog(TestMobCompaction.class);

    private static final HBaseTestingUtility HTU = new HBaseTestingUtility();

    private final static String famStr = "f1";
    private final static byte[] fam = Bytes.toBytes(famStr);
    private final static byte[] qualifier = Bytes.toBytes("q1");
    private final static long mobLen = 10;
    private final static byte[] mobVal = Bytes.toBytes("01234567890123456789012345678901234567890123456789012345678901234567890123456789");
    private final static HTableDescriptor hdt = HTU.createTableDescriptor("testMobCompactTable");
    private static HColumnDescriptor hcd= new HColumnDescriptor(fam);
    private final static long count = 10000000;
    private static Table table = null;

    private static volatile boolean run = true;

    @BeforeClass
    public static void beforeClass() throws Exception {
        HTU.getConfiguration().setInt("hfile.format.version", 3);
        HTU.getConfiguration().setLong(TimeToLiveHFileCleaner.TTL_CONF_KEY, 0);
        HTU.getConfiguration().setInt("hbase.client.retries.number", 100);
        //HTU.getConfiguration().setInt("hbase.hfile.compaction.discharger.interval", 100);
        HTU.getConfiguration().setInt("hbase.hregion.max.filesize", 150000000);
        HTU.getConfiguration().setInt("hbase.hregion.memstore.flush.size", 1000000);
        HTU.startMiniCluster();

        // Create table then get the single region for our new table.
        hcd= new HColumnDescriptor(fam);
        hcd.setMobEnabled(true);
        hcd.setMobThreshold(mobLen);
        hcd.setMaxVersions(1);
        hdt.addFamily(hcd);
        table = HTU.createTable(hdt, null);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        HTU.shutdownMiniCluster();
    }

     class MobCompactionThread implements Runnable {

        @Override
        public void run() {

            while (run) {
                try {
                    //MOB Compaction
                    HTU.getHBaseAdmin().compact(hdt.getTableName(), fam, CompactType.MOB);
                    Thread.sleep(120000);

                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }


     class MajorCompaction implements Runnable {

        @Override
        public void run() {
            while (run) {
                try {
                    HTU.getHBaseAdmin().majorCompact(hdt.getTableName(), fam);
                    Thread.sleep(120000);
                }catch (Exception e){
                    e.printStackTrace();
                }

            }

        }
    }


    class CleanArchive implements Runnable {

        @Override
        public void run() {
            while (run) {
                try {
                    HTU.getMiniHBaseCluster().getMaster().getHFileCleaner().choreForTesting();
                    Thread.sleep(180000);
                }catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }



    class WriteData implements Runnable {

        private long rows=-1;

        public WriteData(long rows) {
            this.rows = rows;
        }

        @Override
        public void run() {
            try {
                HRegion r = HTU.getMiniHBaseCluster().getRegions(hdt.getTableName()).get(0);
                //Put Operation
                for (int i = 0; i < rows; i++) {
                    Put p = new Put(Bytes.toBytes(i));
                    p.addColumn(fam, qualifier, mobVal);
                    table.put(p);
                }
                run = false;
            }catch (Exception e) {
                e.printStackTrace();
            }


        }
    }






    @Test
    public void testMobCompaction() throws InterruptedException, IOException {

        try {

            Thread writeData = new Thread(new WriteData(count));
            writeData.start();

            Thread mobcompact = new Thread(new MobCompactionThread());
            mobcompact.start();


            Thread majorcompact = new Thread(new MajorCompaction());
            majorcompact.start();


            Thread cleanArchive = new Thread(new CleanArchive());
            cleanArchive.start();

            while (run){
                Thread.sleep(30000);
            }
             try {
                    Get get;Result result;
                    for (int i = 0; i < count ; i++) {
                        get = new Get(Bytes.toBytes(i));
                        result = table.get(get);
                        assertTrue(Arrays.equals(result.getValue(fam, qualifier), mobVal));
                    }
                } catch (Exception e) {
                        e.printStackTrace();
                        assertTrue(false);
                }
        } finally {

            HTU.getHBaseAdmin().disableTable(hdt.getTableName());
            HTU.deleteTable(hdt.getTableName());
        }
    }
}
