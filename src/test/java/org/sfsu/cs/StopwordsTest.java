package org.sfsu.cs;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by rajanishivarajmaski1 on 7/8/18.
 */
public class StopwordsTest {

    public static void main(String[] args) throws IOException {
        StopwordsTest stopwords = new StopwordsTest();
        stopwords.fileCompare("/Users/rajanishivarajmaski1/University/csc895/selective-search/src/test/resources/stopwords.txt",
                "/Users/rajanishivarajmaski1/University/csc895/selective-search/20180708_145711_stopwordsReWriteToTest.txt");

    }
    public void fileCompare(String s1, String s2) throws IOException {
        ArrayList<String> stopWords1 = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(
                Paths.get(s1),
                StandardCharsets.UTF_8)) {
            reader.lines().forEach(e -> stopWords1.add(e));
        } catch (IOException ex) {
        }
        Arrays.sort(stopWords1.toArray());
        Set<String> stopWordslist1 = new HashSet<>(stopWords1);
        List sortedList1 = new ArrayList(stopWordslist1);
        Collections.sort(sortedList1);

        Iterator<String> s1Iter = sortedList1.iterator();

        ArrayList<String> stopWords2 = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(
                Paths.get(s2),
                StandardCharsets.UTF_8)) {
            reader.lines().forEach(e -> stopWords2.add(e));
        } catch (IOException ex) {
        }

        Arrays.sort(stopWords1.toArray());
        Set<String> stopWordslist2 = new HashSet<>(stopWords2);
        List sortedList2 = new ArrayList(stopWordslist2);
        Collections.sort(sortedList2);

        Iterator<String> s2Iter = sortedList2.iterator();



        while (s1Iter.hasNext()){
            System.out.println(s1Iter.next());
        }
        System.out.println("\n\n\n");
        System.out.println("second set");
        while (s2Iter.hasNext()){
            System.out.println(s2Iter.next());
        }

       /* while (s1Iter.hasNext()) {
            if (s2Iter.hasNext()) {
                String sp1 = s1Iter.next();
                String sp2 = s2Iter.next();
                if (sp1.equals(sp2)) {
                  //  System.out.println("equals: " + sp1);
                } else {
                    System.out.println("sp1: " + sp1 + "  sp2: "+ sp2);
                }
            }
        }*/

    }

}
