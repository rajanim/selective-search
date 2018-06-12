package org.sfsu.cs;

import com.lucidworks.spark.util.SolrQuerySupport;
import com.lucidworks.spark.util.SolrSupport;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;

import java.io.File;
import java.io.IOException;

/**
 * Created by rajanishivarajmaski1 on 6/10/18.
 */
public class TestIndex {

    public static void main(String[] args) {

        TestIndex testIndex = new TestIndex();
        testIndex.loadFq("/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/qrels_withDocName.txt", "localhost:9983", "clueweb");
        System.exit(0);
    }

    void loadFq(String qrelsFile, String zkHost, String collectionName) {
        int count = 0;
        LineIterator lineIterator;
        String line;
        try {
            lineIterator = FileUtils.lineIterator(new File(qrelsFile), "UTF-8");
            while (lineIterator.hasNext()) {
                line = lineIterator.nextLine();
                String[] qrel = line.split(" ");
                String docId = qrel[2].trim();

                if (!querySolr(zkHost, collectionName, docId)) {
                    System.out.println(docId);
                    count++;
                }

            }

            System.out.println("total not found: " + count);
        } catch (IOException io) {

        }

    }

    boolean querySolr(String zkHost, String collection, String id) {
        SolrClient solrClient = SolrSupport.getCachedCloudClient(zkHost);
        SolrQuery query = new SolrQuery("id:" + id);
        query.setFields("id");
        query.set("collection", collection);
      //  System.out.println(query.toString());
        QueryResponse response = SolrQuerySupport.querySolr(solrClient, query, 0, null).get();
        if (response!=null && !response.getResults().isEmpty()){
                return response.getResults().get(0).get("id").equals(id) ? true :false;
        }
        else
            return false;

    }
}
