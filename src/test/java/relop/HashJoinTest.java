package relop;

import global.AttrOperator;
import global.AttrType;
import helpers.ProvidedTestsHelper;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HashJoinTest extends ProvidedTestsRoot {

  @Before
  public void setUp() throws Exception {
    
  }

  @After
  public void tearDown() throws Exception {
    
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

}
