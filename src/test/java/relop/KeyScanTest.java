package relop;

import global.SearchKey;
import helpers.ProvidedTestsHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by david on 3/9/16.
 */
public class KeyScanTest {

    @Before
    public void setUp() throws Exception {
        ProvidedTestsHelper.create_minibase();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testHasNext() throws Exception {
        System.out.println("\n  ~> test key scan (Age = 53.9)...\n");
        SearchKey key = new SearchKey(53.9F);
        KeyScan keyscan = new KeyScan(ProvidedTestsHelper.getDriversSchema(),
                ProvidedTestsHelper.fillDriversFile().getValue2(),
                key,
                ProvidedTestsHelper.fillDriversFile().getValue1());
        keyscan.execute();
    }
}