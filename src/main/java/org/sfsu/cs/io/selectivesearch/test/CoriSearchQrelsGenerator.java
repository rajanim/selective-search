package org.sfsu.cs.io.selectivesearch.test;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.sfsu.cs.search.query.CoriSelectiveSearchSolr;
import org.sfsu.cs.utils.Utility;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Created by rajanishivarajmaski1 on 5/14/18.
 */
public class CoriSearchQrelsGenerator {

    CoriSelectiveSearchSolr coriSelectiveSearchSolr;
    StringBuffer qTimeBuffer;
   // HashMap<Integer, LinkedList<String>> qrelsFq;
    static int topShards = 1;

    public CoriSearchQrelsGenerator(){

    }
    public CoriSearchQrelsGenerator(String zkHost, String collection, String statCollection){
        coriSelectiveSearchSolr = new CoriSelectiveSearchSolr();
        coriSelectiveSearchSolr.initForVariantTIS(zkHost,collection,statCollection);
        qTimeBuffer = new StringBuffer();

    }


    public static void main(String[] args) {
        String fileName = "CoriSearch_Qrels_ALL_ResultsFile_"+topShards+"_.txt";

        CoriSearchQrelsGenerator coriSearchQrelsGenerator = new CoriSearchQrelsGenerator("localhost:9983", "clueweb_s", "clueweb_qrels_cori");
        HashMap<Integer, LinkedList<String>> qrelsFq=  coriSearchQrelsGenerator.loadFq("/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/qrels_withDocName.txt");
        coriSearchQrelsGenerator.generateResults("/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/all_bow.txt",
                "clueweb_s", "clueweb_qrels_cori", "localhost:9983", "/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/" + fileName, qrelsFq);

        Utility.writeToFile(coriSearchQrelsGenerator.qTimeBuffer.toString(), "/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/" + "QTime_Cori_" + topShards);

        System.exit(0);
    }

    HashMap<Integer, LinkedList<String>> loadFq(String qrelsFile) {

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
                docRel = Integer.parseInt(qrel[3].trim());
                qNum = Integer.parseInt(qrel[0]);
                if (qNum == number) {
                  //  if(docRel>0)
                    docNumbers.add(qrel[2]);
                } else {
                    qrelsFq.put(number, docNumbers);
                    number++;
                    docNumbers = new LinkedList<>();

                }
            }

        } catch (IOException io) {

        }
       return qrelsFq;

    }
    protected void generateResults(String queriesFile, String collection, String statsCollection, String zkHost, String outFile,    HashMap<Integer, LinkedList<String>> qrelsFq) {
        LineIterator lineIterator = null;
        String line;
        try {
            lineIterator = FileUtils.lineIterator(new File(queriesFile), "UTF-8");
            while (lineIterator.hasNext()) {
                line = lineIterator.nextLine();
                String[] idQuery = line.split(":");
                String fq = getFq(idQuery[0].trim(), qrelsFq);
                SolrDocumentList solrDocumentList = querySolr(idQuery[1].trim(), fq, collection, statsCollection, zkHost);
                if (solrDocumentList != null)
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

    protected SolrDocumentList querySolr(String query, String fq, String clusterColl, String coriStatCollection, String zkHost) {
        Tuple2 results = coriSelectiveSearchSolr.executeCoriSelectiveSearch(zkHost, clusterColl,
                coriStatCollection, query, topShards, 0.4, "id, score", 1000, fq);
        QueryResponse response;
        if (results != null) {
            response = (QueryResponse) results._2();

            qTimeBuffer.append((int) results._1()).append("\n");
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
            int rank = 1;
            while (solrDocumentList.hasNext()) {
                SolrDocument document = solrDocumentList.next();
                StringBuilder builder = new StringBuilder();
                builder.append(queryId).append(" ").append("0").append(" ");
                builder.append(document.get("id")).append(" ");
                builder.append(rank++).append(" ");
                builder.append(document.get("score")).append(" ");
                builder.append("tfidf").append("\n");
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
}
