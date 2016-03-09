package relop;

import helpers.ProvidedTestsHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by david on 3/9/16.
 */
public class ProjectionTest {

    @Before
    public void setUp() throws Exception {
        ProvidedTestsHelper.create_minibase();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testGetNext() throws Exception {

        System.out.println("\n  ~> test projection (columns 3 and 1)...\n");
        FileScan scan = new FileScan(ProvidedTestsHelper.getDriversSchema(), ProvidedTestsHelper.fillDriversFile());
        Projection pro = new Projection(scan, 3, 1);
        pro.execute();

    }
}