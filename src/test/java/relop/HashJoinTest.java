package relop;

import global.AttrOperator;
import global.AttrType;
import helpers.FileHelper;
import helpers.ProvidedTestsHelper;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

public class HashJoinTest extends ProvidedTestsRoot {

  private File out;
  private File out2;

  @Before
  public void setUp() throws Exception {
    out = new File("hashjoin.out");
    out2 = new File("hashjoinrestart.out");
  }

  @After
  public void tearDown() throws Exception {
    out.delete();
    out2.delete();
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
  public void testRestart() throws IOException {
    FileScan drivers = ProvidedTestsHelper.hashFillDrivers();
    FileScan rides = ProvidedTestsHelper.hashFillRides();
    HashJoin join = new HashJoin(drivers, rides, 0, 0);

    PrintStream stdout = System.out;
    System.setOut(new PrintStream(out));
    join.execute();

    join.restart();
    System.setOut(new PrintStream(out2));
    join.execute();
    System.setOut(stdout);

    List<String> linesOfFile = FileHelper.getLinesOfFile(out.getAbsolutePath());
    List<String> linesOfFile2 = FileHelper.getLinesOfFile(out2.getAbsolutePath());

    assertEquals(linesOfFile.size(), linesOfFile2.size());
    assertArrayEquals(linesOfFile.toArray(), linesOfFile2.toArray());
  }

}
