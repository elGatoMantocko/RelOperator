package relop;

import global.AttrOperator;
import global.AttrType;
import helpers.ProvidedTestsHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by david on 3/9/16.
 */
public class SimpleJoinTest extends ProvidedTestsRoot {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testGetNext() throws Exception {
        System.out.println("\n  ~> test simple (nested loops) join...\n");

        Predicate[] preds = new Predicate[]{new Predicate(AttrOperator.EQ,
                AttrType.FIELDNO, 0, AttrType.FIELDNO, 5)};
        SimpleJoin join = new SimpleJoin(new FileScan(ProvidedTestsHelper.getDriversSchema(), ProvidedTestsHelper.fillDriversFile().getValue1()),
                new FileScan(ProvidedTestsHelper.getDriversSchema(), ProvidedTestsHelper.fillDriversFile().getValue1()), preds);
        Projection pro = new Projection(join, 0, 1, 5, 6);
        pro.execute();
    }

    @Test
    public void testJoinNoCondition() throws Exception {
        Predicate[] preds = new Predicate[]{new Predicate(AttrOperator.EQ,
                AttrType.FIELDNO, 0, AttrType.FIELDNO, 0)};
        SimpleJoin join = new SimpleJoin(new FileScan(ProvidedTestsHelper.getDriversSchema(), ProvidedTestsHelper.fillDriversFile().getValue1()),
                new FileScan(ProvidedTestsHelper.getDriversSchema(), ProvidedTestsHelper.fillDriversFile().getValue1()), preds);
        join.execute();
    }
}

