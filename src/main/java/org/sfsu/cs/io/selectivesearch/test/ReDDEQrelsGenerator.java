package org.sfsu.cs.io.selectivesearch.test;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.sfsu.cs.search.query.ReDDESelecitveSearch;
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
 * Created by rajanishivarajmaski1 on 5/16/18.
 */
public class ReDDEQrelsGenerator {

    ReDDESelecitveSearch reDDESelecitveSearch;
    StringBuffer qTimeBuffer = new StringBuffer();
    public ReDDEQrelsGenerator() {
        reDDESelecitveSearch = new ReDDESelecitveSearch();
    }

    public static void main(String[] args) {
        String fileName = "ReddeSearch_All_ResultsFile_qrels_10Shards.txt";
        ReDDEQrelsGenerator reDDEQrelsGenerator = new ReDDEQrelsGenerator();
        int rows = 1000;

        HashMap<Integer, LinkedList<String>> qrelsFq=  reDDEQrelsGenerator.loadFqForRel(
                "/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/qrels_withDocName.txt");

        reDDEQrelsGenerator.generateResults("/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/all_bow.txt",
                "clueweb_s", "clueweb_qrels_redde", "localhost:9983", "/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/" + fileName, rows, qrelsFq);

        //Utility.writeToFile(reDDEQrelsGenerator.qTimeBuffer.toString(), "/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/"+"QTime_ReDDE");

        System.exit(0);
    }
    protected void generateResults(String queriesFile, String collection, String statsCollection, String zkHost, String outFile, int rowsToRet, HashMap<Integer, LinkedList<String>>  qrelsFq) {
        LineIterator lineIterator = null;
        String line;
        try {
            lineIterator = FileUtils.lineIterator(new File(queriesFile), "UTF-8");
            while (lineIterator.hasNext()) {
                line = lineIterator.nextLine();
                String[] idQuery = line.split(":");
                String fq = getFq(idQuery[0].trim(), qrelsFq);
                SolrDocumentList solrDocumentList = querySolr(idQuery[1].trim(), collection, statsCollection, zkHost, rowsToRet, fq);
                if (solrDocumentList != null)
                    appendResultsToFile(idQuery[0].trim(), solrDocumentList.iterator(), outFile);
            }
        } catch (IOException e) {

        } finally {
            LineIterator.closeQuietly(lineIterator);
        }



    }

    protected SolrDocumentList querySolr(String query, String clusterColl, String statCollection, String zkHost, int rowsToRet, String fq) {
        Tuple2 response = reDDESelecitveSearch.relevantDDEBasedSelectiveSearch(zkHost, statCollection, clusterColl,
                query, 1000, 10, rowsToRet,fq);
        if(response!=null) {
            qTimeBuffer.append((int) (response._1())).append("\n");
            return response != null ? (SolrDocumentList) response._2() : null;
        }else{
            return null;
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
                   // if(docRel>0)
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
