package com.ebay.test;

import com.lucidworks.spark.util.SolrQuerySupport;
import com.lucidworks.spark.util.SolrSupport;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Created by rajanishivarajmaski1 on 4/5/18.
 */
public class SimilarTitlesSearcher {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        String inputDirPath = "/Users/rajanishivarajmaski1/Desktop/ebay_test";
        SimilarTitlesSearcher similarTitlesSearcher = new SimilarTitlesSearcher();
        similarTitlesSearcher.searchSimilarItemsWriteOut(inputDirPath);
        System.out.println("end of program");
    }

    /**
     * Loop through each file from the given directory  -> loop through each line in the file and obtain title ->
     * search for similar titles from the given category and write out output
     *
     * @param inputDirPath
     * @throws IOException
     */
    public void searchSimilarItemsWriteOut(String inputDirPath) throws IOException {
        File dir = new File(inputDirPath);
        System.out.println("Getting all files in " + dir.getCanonicalPath());
        List<File> files = (List<File>) FileUtils.listFiles(dir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
        for (File file : files) {

            System.out.println("file: " + file.getCanonicalPath());
            if(!file.isHidden())
            searchSimilarItems(file.getCanonicalPath());

        }
    }

    /** search for similar titles
     * @param fileName
     * @throws IOException
     */
    public void searchSimilarItems(String fileName) throws IOException {
        //read file into stream, try-with-resources
        try (Stream<String> stream = Files.lines(Paths.get(fileName))) {

            stream.forEach(line -> {
                try {
                    getTopTitlesWriteOut(line);

                } catch (IOException e) {
                    e.printStackTrace();
                }

            });

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     *
     * @param line
     * @return
     * @throws IOException
     */
    public List<String> getTopTitlesWriteOut(String line) throws IOException {
        String tokens[] = line.split("\t");
        List<String> topTitles = new LinkedList<>();
        SolrQuery solrQuery = getSolrquery(tokens[2], tokens[3]);
        SolrClient solrClient = SolrSupport.getCachedCloudClient("localhost:9983");
        System.out.println(solrQuery.toString());
        QueryResponse response = SolrQuerySupport.querySolr(solrClient, solrQuery, 0, null).get();
        response.getResults().forEach(doc -> topTitles.add(doc.get("title_txt").toString()));
        writeOutToFile(tokens[2]+ "_" + tokens[0], tokens[3], topTitles);
        return topTitles;
    }

    void writeOutToFile(String fileName, String searchTitle, List<String> titles) throws IOException {
        String filePath = "/Users/rajanishivarajmaski1/Desktop/ebay_op/" + fileName;
        FileWriter writer = new FileWriter(filePath);
        writer.write("Search Title:\n"+ searchTitle + "\n\n");
        for (String str : titles) {
            writer.write(str + "\n\n");
        }
        writer.close();

    }

    public SolrQuery getSolrquery(String cid, String title) {
        String queryTerms = getQueryTerms(title);
        SolrQuery solrQuery = new SolrQuery(queryTerms);
        solrQuery.set("collection", "ebay");
        solrQuery.addFilterQuery("cid_s:" + cid);
        return solrQuery;
    }

    /**
     * Remove special characters and retrieve only terms
     * @param title
     * @return
     */
    private String getQueryTerms(String title) {
        StringBuffer buffer = new StringBuffer();
        String[] terms = title.toLowerCase().split("\\W+");
        for (String term : terms) {
            buffer.append(term).append(" ");
        }
        return buffer.toString();
    }
}
