package helpers;

import global.AttrType;
import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import relop.IndexScan;

import relop.FileScan;
import relop.Schema;
import relop.Tuple;

import java.util.Random;

/**
 * A helper class for all the initializations required by the provided tests.
 *
 * It can create the schemas needed in the provided tests as well as create the minibase itself.
 */
public class ProvidedTestsHelper {

    /** Default database file name. */
    protected static String DB_PATH = System.getProperty("user.name") + ".minibase";

    /** Default database size (in pages). */
    protected static int DB_SIZE = 10000;

	/** Size of tables in test3. */
	private static final int SUPER_SIZE = 2000;

    /** Default buffer pool size (in pages) */
    protected static int BUF_SIZE = 100;

    /** Default buffer pool replacement policy */
    protected static String BUF_POLICY = "Clock";


    /** Drivers table schema. */
    private static Schema s_drivers;

    /** Rides table schema. */
    private static Schema s_rides;

    /** Groups table schema. */
    private static Schema s_groups;

    public static void create_minibase() {
        System.out.println("Creating database...\nReplacer: " + BUF_POLICY);
        new Minibase(System.getProperty("user.name") + new Random().nextInt() + ".minibase", DB_SIZE, BUF_SIZE, BUF_POLICY, false);
    }

    public static Schema getDriversSchema() {
        if(s_drivers == null) initDriversSchema();
        return s_drivers;
    }
    public static Schema getGroupsSchema() {
        if(s_groups == null) initGroupsSchema();
        return s_groups;
    }
    public static Schema getRidesSchema() {
        if(s_rides == null) initRidesSchema();
        return s_rides;
    }

    public static Schema initDriversSchema() {
        s_drivers = new Schema(5);
        s_drivers.initField(0, AttrType.INTEGER, 4, "DriverId");
        s_drivers.initField(1, AttrType.STRING, 20, "FirstName");
        s_drivers.initField(2, AttrType.STRING, 20, "LastName");
        s_drivers.initField(3, AttrType.FLOAT, 4, "Age");
        s_drivers.initField(4, AttrType.INTEGER, 4, "NumSeats");
        return s_drivers;
    }

    public static Schema initRidesSchema() {
        s_rides = new Schema(4);
        s_rides.initField(0, AttrType.INTEGER, 4, "DriverId");
        s_rides.initField(1, AttrType.INTEGER, 4, "GroupId");
        s_rides.initField(2, AttrType.STRING, 10, "FromDate");
        s_rides.initField(3, AttrType.STRING, 10, "ToDate");
        return s_rides;
    }

    public static Schema initGroupsSchema() {
        s_groups = new Schema(2);
        s_groups.initField(0, AttrType.INTEGER, 4, "GroupId");
        s_groups.initField(1, AttrType.STRING, 10, "Country");
        return s_groups;
    }

    public static Pair<HeapFile, HashIndex> fillDriversFile() {
        // create and populate a temporary Drivers file and index
        Tuple tuple = new Tuple(getDriversSchema());
        HeapFile file = new HeapFile(null);
        HashIndex index = new HashIndex(null);
        for (int i = 1; i <= 10; i++) {

            // create the tuple
            tuple.setIntFld(0, i);
            tuple.setStringFld(1, "f" + i);
            tuple.setStringFld(2, "l" + i);
            Float age = (float) (i * 7.7);
            tuple.setFloatFld(3, age);
            tuple.setIntFld(4, i + 100);

            // insert the tuple in the file and index
            RID rid = file.insertRecord(tuple.getData());
            index.insertEntry(new SearchKey(age), rid);

        } // for
        return new Pair<HeapFile, HashIndex>(file, index);
    }

    public static FileScan hashFillDrivers() {
      HeapFile drivers = new HeapFile(null);
      Tuple tuple = new Tuple(getDriversSchema());
      tuple.setAllFields(1, "Ahmed", "Elmagarmid", 25F, 5);
      tuple.insertIntoFile(drivers);
      tuple.setAllFields(2, "Walid", "Aref", 27F, 13);
      tuple.insertIntoFile(drivers);
      tuple.setAllFields(3, "Christopher", "Clifton", 18F, 4);
      tuple.insertIntoFile(drivers);
      tuple.setAllFields(4, "Sunil", "Prabhakar", 22F, 7);
      tuple.insertIntoFile(drivers);
      tuple.setAllFields(5, "Elisa", "Bertino", 26F, 5);
      tuple.insertIntoFile(drivers);
      tuple.setAllFields(6, "Susanne", "Hambrusch", 23F, 3);
      tuple.insertIntoFile(drivers);
      tuple.setAllFields(7, "David", "Eberts", 24F, 8);
      tuple.insertIntoFile(drivers);
      tuple.setAllFields(8, "Arif", "Ghafoor", 20F, 5);
      tuple.insertIntoFile(drivers);
      tuple.setAllFields(9, "Jeff", "Vitter", 19F, 10);
      tuple.insertIntoFile(drivers);

      return new FileScan(getDriversSchema(), drivers);
    }

    public static FileScan hashFillRides() {
      HeapFile rides = new HeapFile(null);
      Tuple tuple = new Tuple(getRidesSchema());
      tuple.setAllFields(3, 5, "2/10/2006", "2/13/2006");
      tuple.insertIntoFile(rides);
      tuple.setAllFields(1, 2, "2/12/2006", "2/14/2006");
      tuple.insertIntoFile(rides);
      tuple.setAllFields(9, 1, "2/15/2006", "2/15/2006");
      tuple.insertIntoFile(rides);
      tuple.setAllFields(5, 7, "2/14/2006", "2/18/2006");
      tuple.insertIntoFile(rides);
      tuple.setAllFields(1, 3, "2/15/2006", "2/16/2006");
      tuple.insertIntoFile(rides);
      tuple.setAllFields(2, 6, "2/17/2006", "2/20/2006");
      tuple.insertIntoFile(rides);
      tuple.setAllFields(3, 4, "2/18/2006", "2/19/2006");
      tuple.insertIntoFile(rides);
      tuple.setAllFields(4, 1, "2/19/2006", "2/19/2006");
      tuple.insertIntoFile(rides);
      tuple.setAllFields(2, 7, "2/18/2006", "2/23/2006");
      tuple.insertIntoFile(rides);
      tuple.setAllFields(8, 5, "2/20/2006", "2/22/2006");
      tuple.insertIntoFile(rides);
      tuple.setAllFields(3, 2, "2/24/2006", "2/26/2006");
      tuple.insertIntoFile(rides);
      tuple.setAllFields(6, 6, "2/25/2006", "2/26/2006");
      tuple.insertIntoFile(rides);

      return new FileScan(getRidesSchema(), rides);
    }

    public static IndexScan getLargeDriversFile() {
      Tuple tuple = new Tuple(getDriversSchema());
      HeapFile drivers = new HeapFile(null);
      HashIndex ixdrivers = new HashIndex(null);
      for (int i = 1; i <= SUPER_SIZE; i++) {

        // create the tuple
        tuple.setIntFld(0, i);
        tuple.setStringFld(1, "f" + i);
        tuple.setStringFld(2, "l" + i);
        tuple.setFloatFld(3, (float) (i * 7.7));
        tuple.setIntFld(4, i + 100);

        // insert the tuple in the file and index
        RID rid = drivers.insertRecord(tuple.getData());
        ixdrivers.insertEntry(new SearchKey(i), rid);

      } // for

      return new IndexScan(getDriversSchema(), ixdrivers, drivers);
    }

    public static FileScan getLargeGroupFile() {
      Tuple tuple = new Tuple(getGroupsSchema());
      HeapFile groups = new HeapFile(null);
      for (int i = 1; i <= SUPER_SIZE / 10; i++) {
          tuple.setAllFields(i, "Purdue");
          tuple.insertIntoFile(groups);
      }

      return new FileScan(getGroupsSchema(), groups);
    }

    public static FileScan getLargeRidesFile() {
      Tuple tuple = new Tuple(getRidesSchema());
      HeapFile rides = new HeapFile(null);
      Random random = new Random(74);
      for (int i = 1; i <= SUPER_SIZE; i++) {
          // random relationships between drivers and groups
          int r1 = Math.abs(random.nextInt() % SUPER_SIZE + 1);
          int r2 = Math.abs(random.nextInt() % (SUPER_SIZE / 10) + 1);
          tuple.setAllFields(r1, r2, "3/27/2006", "4/7/2006");
          tuple.insertIntoFile(rides);
      }

      return new FileScan(getRidesSchema(), rides);
    }
    
}

