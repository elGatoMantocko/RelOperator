package relop;

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
    HashJoin join = new HashJoin(ProvidedTestsHelper.hashFillDrivers(), ProvidedTestsHelper.hashFillRides(), 0, 0);
    join.execute();
  }

}
