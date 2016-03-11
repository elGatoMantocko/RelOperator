package relop;

import diskmgr.DiskMgr;
import global.Minibase;
import helpers.ProvidedTestsHelper;
import org.junit.After;
import org.junit.Before;

/**
 * Created by david on 3/11/16.
 */
public class ProvidedTestsRoot {
    @Before
    public void initDB() throws Exception {
        ProvidedTestsHelper.create_minibase();
    }

    @After
    public void destroyDB() throws Exception {
        Minibase.DiskManager.closeDB();
        Minibase.DiskManager.destroyDB();
    }
}
