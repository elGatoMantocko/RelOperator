package relop;

import global.AttrOperator;
import global.AttrType;
import helpers.FileHelper;
import helpers.ProvidedTestsHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class HashJoinTest extends ProvidedTestsRoot {
  File simpleout;
  File hashout;

  @Before
  public void setUp() throws Exception {
    hashout = new File("hashjoin.out");
    simpleout = new File("simplejoin.out");

  }

  @After
  public void tearDown() throws Exception {
    //hashout.delete();
    //simpleout.delete();
  }

  @Test
  public void joinSmallTables() {
    FileScan drivers = ProvidedTestsHelper.hashFillDrivers();
    FileScan rides = ProvidedTestsHelper.hashFillRides();
    HashJoin join = new HashJoin(drivers, rides, 0, 0);
    join.execute();
  }

  @Test
  public void subJoinOperation() {
    IndexScan drivers = ProvidedTestsHelper.getLargeDriversFile();
    FileScan rides = ProvidedTestsHelper.getLargeRidesFile();
    FileScan groups = ProvidedTestsHelper.getLargeGroupFile();

    HashJoin join1 = new HashJoin(groups, rides, 0, 1);
    HashJoin join2 = new HashJoin(join1, drivers, 2, 0);
	Selection sel = new Selection(join2, new Predicate(AttrOperator.LT, AttrType.FIELDNO, 10, AttrType.FIELDNO, 0));
    sel.execute();
  }

  @Test
  public void countOutputTest() throws IOException {
    PrintStream stdout = System.out;

    System.setOut(new PrintStream(hashout));
    FileScan drivers = ProvidedTestsHelper.hashFillDrivers();
    FileScan rides = ProvidedTestsHelper.hashFillRides();
    HashJoin join = new HashJoin(drivers, rides, 0, 0);
    join.execute();

    System.setOut(new PrintStream(simpleout));
    Predicate[] preds = new Predicate[]{new Predicate(AttrOperator.EQ,
            AttrType.FIELDNO, 0, AttrType.FIELDNO, 5)};
    SimpleJoin sjoin = new SimpleJoin(new FileScan(ProvidedTestsHelper.getDriversSchema(), ProvidedTestsHelper.fillDriversFile().getValue1()),
            new FileScan(ProvidedTestsHelper.getDriversSchema(), ProvidedTestsHelper.fillDriversFile().getValue1()), preds);
    sjoin.execute();
    System.setOut(stdout);

    List<String> linesOfHashJoin = FileHelper.getLinesOfFile(hashout.getAbsolutePath());
    List<String> linesOfSimpleJoin = FileHelper.getLinesOfFile(simpleout.getAbsolutePath());

    assertEquals("Should have same number of tuples", linesOfSimpleJoin.size(), linesOfHashJoin.size());

    //Tuples should be the same and in the same order.
    for(int i = 0; i < linesOfHashJoin.size(); i++) {
      assertEquals("Lines should be the same", linesOfSimpleJoin.get(i), linesOfHashJoin.get(i));
    }
  }

}
