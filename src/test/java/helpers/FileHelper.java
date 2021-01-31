package helpers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by david on 3/18/16.
 */
public class FileHelper {
    public static List<String> getLinesOfFile(String filename) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        List<String> list=new ArrayList<String>();
        String line="";
        while((line=reader.readLine())!=null){
            list.add(line);
        }
        reader.close();
        Collections.sort(list);
        return list;
    }
}
