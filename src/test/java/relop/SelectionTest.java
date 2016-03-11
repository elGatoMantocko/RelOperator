package relop;

import global.AttrOperator;
import global.AttrType;
import helpers.ProvidedTestsHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by david on 3/9/16.
 */
public class SelectionTest {

    @Before
    public void setUp() throws Exception {
        ProvidedTestsHelper.create_minibase();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void getNextSimple() throws Exception {
        System.out.println("\n  ~> test selection (Age > 65 OR Age < 15)...\n");
        Predicate[] preds = new Predicate[] {
          new Predicate(AttrOperator.GT, AttrType.FIELDNO, 3, AttrType.FLOAT, 65F),
          new Predicate(AttrOperator.LT, AttrType.FIELDNO, 3, AttrType.FLOAT, 15F)
        };
        FileScan scan = new FileScan(ProvidedTestsHelper.getDriversSchema(), ProvidedTestsHelper.fillDriversFile().getValue1());
        Selection sel = new Selection(scan, preds);
        sel.execute();
    }
}

