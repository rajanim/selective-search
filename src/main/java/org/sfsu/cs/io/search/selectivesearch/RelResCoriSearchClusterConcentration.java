package org.sfsu.cs.io.search.selectivesearch;

import com.lucidworks.spark.util.SolrQuerySupport;
import com.lucidworks.spark.util.SolrSupport;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.sfsu.cs.search.query.CoriSelectiveSearchSolr;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Created by rajanishivarajmaski1 on 7/28/18.
 */
public class RelResCoriSearchClusterConcentration {

    CoriSelectiveSearchSolr coriSelectiveSearchSolr;

    public static void main(String[] args) {
        RelResCoriSearchClusterConcentration relResultsConcentration = new RelResCoriSearchClusterConcentration();
        int rows = 1000;
        int topShards = 5;
        String outFile = "/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/" + "aug_12_RelResultsCoriSearchClustersN35k_K50_I10_T5.txt";
        HashMap<Integer, LinkedList<String>> qrelsFq=  relResultsConcentration.loadFqForRel(
                "/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/qrels_withDocName.txt");

        relResultsConcentration.generateResults("/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/all_bow.txt",
                "clueweb_s", "clueweb_qrels_cori","localhost:9983",outFile,qrelsFq, topShards, rows);

    }


    protected void generateResults(String queriesFile, String collection, String coriStatColl, String zkHost, String outFile,
                                   HashMap<Integer, LinkedList<String>>  qrelsFq,  int topShards, int rows) {
        coriSelectiveSearchSolr = new CoriSelectiveSearchSolr();
        coriSelectiveSearchSolr.initForVariantTIS(zkHost,collection,coriStatColl);

        LineIterator lineIterator = null;
        String line;
        try {
            lineIterator = FileUtils.lineIterator(new File(queriesFile), "UTF-8");
            while (lineIterator.hasNext()) {
                line = lineIterator.nextLine();
                String[] idQuery = line.split(":");
                String fq = getFq(idQuery[0].trim(), qrelsFq);
                SolrDocumentList solrDocumentList = querySolr(idQuery[1].trim(), fq, collection,coriStatColl, zkHost, topShards, rows);
                if(solrDocumentList!=null)
                appendResultsToFile(line, solrDocumentList.iterator(), outFile);
            }
        } catch (IOException e) {

        } finally {
            LineIterator.closeQuietly(lineIterator);
        }

    }

    String getFq(String id, HashMap<Integer, LinkedList<String>> qrelsFq) {
        LinkedList<String> ids = qrelsFq.get(Integer.parseInt(id));
        StringBuffer fqQuery = new StringBuffer();
        fqQuery.append("(");
        for (String str : ids) {
            fqQuery.append("\"").append(str).append("\"").append(" OR ");
        }
        fqQuery.append("id:0)");
        //System.out.println(fqQuery.toString());
        return fqQuery.toString();
    }

    protected SolrDocumentList querySolr(String query, String fq, String clusterColl, String coriStatCollection, String zkHost, int topShards, int rows) {

        Tuple2 results = coriSelectiveSearchSolr.executeCoriSelectiveSearch(zkHost, clusterColl,
                coriStatCollection, query, topShards, 0.4, "id, score, clusterId_i", rows, fq);
        QueryResponse response;
        if (results != null) {
            response = (QueryResponse) results._2();

            return response.getResults();
        } else
            return null;
    }

    protected void appendResultsToFile(String queryId, Iterator<SolrDocument> solrDocumentList, String outFile) {
        BufferedWriter bw = null;
        FileWriter fw = null;
        try {
            File file = new File(outFile);
            // if file doesnt exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }

            // true = append file
            fw = new FileWriter(file.getAbsoluteFile(), true);
            bw = new BufferedWriter(fw);
            StringBuilder builder = new StringBuilder();
            builder.append(queryId).append(" ");
            builder.append("\n");
            int[] array = new int[51];
            while (solrDocumentList.hasNext()) {
                SolrDocument document = solrDocumentList.next();
                int clusterId = Integer.parseInt(document.get("clusterId_i").toString());
                array[clusterId]++;
            }
            for(int i =1; i < array.length ; i++){
                if(array[i]>0)
                builder.append(i + " " + array[i]).append("\n");

            }
            builder.append("\n");
            bw.write(builder.toString());

            // System.out.println("Done");

        } catch (IOException e) {

            e.printStackTrace();

        } finally {

            try {

                if (bw != null)
                    bw.close();

                if (fw != null)
                    fw.close();

            } catch (IOException ex) {

                ex.printStackTrace();

            }
        }

    }

    HashMap<Integer, LinkedList<String>> loadFqForRel(String qrelsFile) {

        LineIterator lineIterator;
        HashMap<Integer, LinkedList<String>>   qrelsFq = new HashMap<>();
        String line;
        int number = 1;
        int qNum;
        int docRel;
        try {
            lineIterator = FileUtils.lineIterator(new File(qrelsFile), "UTF-8");
            LinkedList<String> docNumbers = new LinkedList<>();
            while (lineIterator.hasNext()) {
                line = lineIterator.nextLine();
                String[] qrel = line.split(" ");
                qNum = Integer.parseInt(qrel[0]);
                docRel = Integer.parseInt(qrel[3].trim());
                if (qNum == number) {
                    if(docRel>0)
                    docNumbers.add(qrel[2]);
                } else {
                    qrelsFq.put(number, docNumbers);
                    number++;
                    docNumbers = new LinkedList<>();
                    if(docRel>0)
                        docNumbers.add(qrel[2]);

                }
            }

        } catch (IOException io) {

        }
        return qrelsFq;

    }


}
