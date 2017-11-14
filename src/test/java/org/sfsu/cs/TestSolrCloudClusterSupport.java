package org.sfsu.cs;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Created by rajanishivarajmaski1 on 11/13/17.
 */
public class TestSolrCloudClusterSupport {

    public static String readSolrXml(File solrXml) throws IOException {
        StringBuilder sb = new StringBuilder();
        String line;
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(solrXml), StandardCharsets.UTF_8));
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException ignore){}
            }
        }
        return sb.toString();
    }
}
