package helpers;

import global.AttrType;
import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import relop.Schema;
import relop.Tuple;

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
        new Minibase(DB_PATH, DB_SIZE, BUF_SIZE, BUF_POLICY, false);
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
}
