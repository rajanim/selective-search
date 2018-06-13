package org.sfsu.cs.io.selectivesearch.test;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.IOException;

/**
 * Created by rajanishivarajmaski1 on 6/12/18.
 */
public class QrelsDocsCounter {

    public static void main(String[] args) {
        int count = 0;
        LineIterator lineIterator;
        String qrelsFile = "/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/qrels_withDocName.txt";
        String line;
        try {
            lineIterator = FileUtils.lineIterator(new File(qrelsFile), "UTF-8");
            while (lineIterator.hasNext()) {
                line = lineIterator.nextLine();
                String[] qrel = line.split(" ");
                String queryId = qrel[0].trim();
                if (queryId.trim().equals("201")) {
                    System.out.println("done");
                    System.out.println("total count : " + count);
                    System.exit(0);
                }
                count++;

            }

        } catch (IOException io) {

        }

    }

}
