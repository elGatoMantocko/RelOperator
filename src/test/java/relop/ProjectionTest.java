package relop;

import helpers.ProvidedTestsHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by david on 3/9/16.
 */
public class ProjectionTest extends ProvidedTestsRoot {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testSimpleGetNext() throws Exception {

        System.out.println("\n  ~> test projection (columns 3 and 1)...\n");
        FileScan scan = new FileScan(ProvidedTestsHelper.getDriversSchema(), ProvidedTestsHelper.fillDriversFile().getValue1());
        Projection pro = new Projection(scan, 3, 1);
        pro.execute();

    }
}