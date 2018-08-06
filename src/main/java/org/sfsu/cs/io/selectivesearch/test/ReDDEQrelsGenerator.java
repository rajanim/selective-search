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
import java.util.Iterator;

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
        String fileName = "1M_ReddeSearch_ResultsFile_10Shards_10Docs.txt";
        ReDDEQrelsGenerator reDDEQrelsGenerator = new ReDDEQrelsGenerator();
        int rows = 10;

        reDDEQrelsGenerator.generateResults("/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/all_bow.txt",
                "clueweb", "clueweb_redde", "localhost:9983", "/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/" + fileName, rows);

        Utility.writeToFile(reDDEQrelsGenerator.qTimeBuffer.toString(), "/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/"+"QTime_ReDDE");

        System.exit(0);
    }
    protected void generateResults(String queriesFile, String collection, String statsCollection, String zkHost, String outFile, int rowsToRet) {
        LineIterator lineIterator = null;
        String line;
        try {
            lineIterator = FileUtils.lineIterator(new File(queriesFile), "UTF-8");
            while (lineIterator.hasNext()) {
                line = lineIterator.nextLine();
                String[] idQuery = line.split(":");
                SolrDocumentList solrDocumentList = querySolr(idQuery[1].trim(), collection, statsCollection, zkHost, rowsToRet);
                if (solrDocumentList != null)
                    appendResultsToFile(idQuery[0].trim(), solrDocumentList.iterator(), outFile);
            }
        } catch (IOException e) {

        } finally {
            LineIterator.closeQuietly(lineIterator);
        }



    }

    protected SolrDocumentList querySolr(String query, String clusterColl, String statCollection, String zkHost, int rowsToRet) {
        Tuple2 response = reDDESelecitveSearch.relevantDDEBasedSelectiveSearch(zkHost, statCollection, clusterColl,
                query, 10, 20, rowsToRet,"");
        if(response!=null) {
            qTimeBuffer.append((int) (response._1())).append("\n");
            return response != null ? (SolrDocumentList) response._2() : null;
        }else{
            return null;
        }
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
