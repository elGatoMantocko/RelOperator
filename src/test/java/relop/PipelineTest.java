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
public class PipelineTest extends ProvidedTestsRoot {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testSimpleGetNext() throws Exception {

        System.out.println("\n  ~> selection and projection (pipelined)...\n");
        FileScan scan = new FileScan(ProvidedTestsHelper.getDriversSchema(), ProvidedTestsHelper.fillDriversFile().getValue1());

        Predicate[] preds = new Predicate[] {
                new Predicate(AttrOperator.GT, AttrType.FIELDNO, 3, AttrType.FLOAT, 65F),
                new Predicate(AttrOperator.LT, AttrType.FIELDNO, 3, AttrType.FLOAT, 15F)
        };

        Selection sel = new Selection(scan, preds);
        Projection pro = new Projection(sel, 3, 1);
        pro.execute();

    }
}
