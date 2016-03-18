package relop;

import global.SearchKey;
import helpers.FileHelper;
import helpers.ProvidedTestsHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Created by david on 3/9/16.
 */
public class KeyScanTest extends ProvidedTestsRoot {
    KeyScan keyscan;
    File out;
    File out2;

    @Before
    public void setUp() throws Exception {
        out = new File("keyscan.out");
        out2 = new File("keyscanrestart.out");

        SearchKey key = new SearchKey(53.9F);
        keyscan = new KeyScan(ProvidedTestsHelper.getDriversSchema(),
                ProvidedTestsHelper.fillDriversFile().getValue2(),
                key,
                ProvidedTestsHelper.fillDriversFile().getValue1());
    }

    @After
    public void tearDown() throws Exception {
        out.delete();
        out2.delete();
    }

    @Test
    public void testHasNext() throws Exception {
        System.out.println("\n  ~> test key scan (Age = 53.9)...\n");
        keyscan.execute();
    }

    @Test
    public void testRestart() throws IOException {
        PrintStream stdout = System.out;
        System.setOut(new PrintStream(out));
        keyscan.execute();

        keyscan.restart();
        System.setOut(new PrintStream(out2));
        keyscan.execute();
        System.setOut(stdout);

        List<String> linesOfFile = FileHelper.getLinesOfFile(out.getAbsolutePath());
        List<String> linesOfFile2 = FileHelper.getLinesOfFile(out2.getAbsolutePath());

        assertEquals(linesOfFile.size(), linesOfFile2.size());
        assertArrayEquals(linesOfFile.toArray(), linesOfFile2.toArray());
    }
}

