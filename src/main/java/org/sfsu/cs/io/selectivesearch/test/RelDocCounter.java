package org.sfsu.cs.io.selectivesearch.test;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.IOException;

/**
 * Created by rajanishivarajmaski1 on 8/18/18.
 */
public class RelDocCounter {

    public static void main(String[] args) {
        int count = 0;
        LineIterator lineIterator;
        String qrelsFile = "/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/qrels_withDocName.txt";
        String line;
        try {
            lineIterator = FileUtils.lineIterator(new File(qrelsFile), "UTF-8");
            int qNum;
            int docRel;
            int number = 1;
            int relCount = 0;
            while (lineIterator.hasNext()) {
                line = lineIterator.nextLine();
                String[] qrel = line.split(" ");
                qNum = Integer.parseInt(qrel[0]);
                String doc = qrel[2];
                docRel = Integer.parseInt(qrel[3].trim());
                if (qNum == number) {
                    if (docRel > 0)
                        relCount += 1;
                } else {
                    System.out.println( number + " " +relCount);
                    number++;
                    relCount = 0;
                    if (docRel > 0)
                        relCount += 1;

                }
            }
        } catch (IOException io) {

        }

    }
}
