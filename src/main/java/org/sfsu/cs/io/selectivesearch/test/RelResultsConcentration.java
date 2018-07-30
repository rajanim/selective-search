package org.sfsu.cs.io.selectivesearch.test;

import com.lucidworks.spark.util.SolrQuerySupport;
import com.lucidworks.spark.util.SolrSupport;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

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
public class RelResultsConcentration {


    public static void main(String[] args) {
        RelResultsConcentration relResultsConcentration = new RelResultsConcentration();
        String outFile = "/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/" + "RelResultsClustersN30k_K50_I25.txt";
        HashMap<Integer, LinkedList<String>> qrelsFq=  relResultsConcentration.loadFqForRel(
                "/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/qrels_withDocName.txt");

        relResultsConcentration.generateResults("/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/all_bow.txt",
                "clueweb_s","localhost:9983",outFile, 10000,qrelsFq);

    }


    protected void generateResults(String queriesFile, String collection, String zkHost, String outFile, int rows,
                                   HashMap<Integer, LinkedList<String>>  qrelsFq) {
        LineIterator lineIterator = null;
        String line;
        try {
            lineIterator = FileUtils.lineIterator(new File(queriesFile), "UTF-8");
            while (lineIterator.hasNext()) {
                line = lineIterator.nextLine();
                String[] idQuery = line.split(":");
                String fq = getFq(idQuery[0].trim(), qrelsFq);
                SolrDocumentList solrDocumentList = querySolr(idQuery[1].trim(), collection, zkHost, rows,fq);
                appendResultsToFile(idQuery[0].trim(), solrDocumentList.iterator(), outFile);
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

    protected SolrDocumentList querySolr(String query, String collection, String zkHost, int rows, String fq) {
        SolrClient solrClient = SolrSupport.getCachedCloudClient(zkHost);
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.set("collection", collection);
        solrQuery.setQuery(query);
        solrQuery.setRequestHandler("edismax");
        solrQuery.set("fl", "id, clusterId_i, score");
        solrQuery.setRows(rows);
        solrQuery.setFilterQueries(fq);
        QueryResponse collResp = SolrQuerySupport.querySolr(solrClient, solrQuery, 0, null).get();
        return collResp.getResults();
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
            int rank = 1;
            while (solrDocumentList.hasNext()) {
                SolrDocument document = solrDocumentList.next();
                StringBuilder builder = new StringBuilder();
                builder.append(queryId).append(" ").append("0").append(" ");
                builder.append(document.get("id")).append(" ");
                builder.append(rank++).append(" ");
                builder.append(document.get("score")).append(" ");
                builder.append(document.get("clusterId_i")).append("\n");
                bw.write(builder.toString());
            }

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
