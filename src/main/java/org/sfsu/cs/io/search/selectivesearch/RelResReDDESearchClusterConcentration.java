package org.sfsu.cs.io.search.selectivesearch;

import com.google.common.primitives.Ints;
import com.lucidworks.spark.util.SolrQuerySupport;
import com.lucidworks.spark.util.SolrSupport;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.sfsu.cs.search.query.ReDDESelecitveSearch;
import org.sfsu.cs.utils.Utility;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by rajanishivarajmaski1 on 7/28/18.
 */
public class RelResReDDESearchClusterConcentration {

    ReDDESelecitveSearch reDDESelecitveSearch;

    String missingDocsOutFile = "/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/"
            + "_FlatMissingDocsRelClusterConcentration.txt";
    public static void main(String[] args) {
        RelResReDDESearchClusterConcentration relResultsConcentration = new RelResReDDESearchClusterConcentration();
        int rows = 1000;
        int topShards = 10;
        int numCSRows = 5000;
        String outFile = "/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/"
                + "_WithCIDCntRelClusterConcentration.txt";

        HashMap<Integer, LinkedList<String>> qrelsFq = relResultsConcentration.loadFqForRel(
                "/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/qrels_withDocName.txt");

        relResultsConcentration.generateResults("/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/all_bow.txt",
                "clueweb_s", "clueweb_qrels_redde", "localhost:9983", outFile, qrelsFq, numCSRows, topShards, rows);
        System.out.println("done");
        System.exit(0);

    }

    protected void generateResults(String queriesFile, String collection, String coriStatColl, String zkHost, String outFile,
                                   HashMap<Integer, LinkedList<String>> qrelsFq, int numRowsCSIndx, int topShards, int rows) {
        reDDESelecitveSearch = new ReDDESelecitveSearch();
        LineIterator lineIterator = null;
        String line;
        try {
            lineIterator = FileUtils.lineIterator(new File(queriesFile), "UTF-8");
            while (lineIterator.hasNext()) {
                line = lineIterator.nextLine();
                String[] idQuery = line.split(":");
                LinkedList<String> ids = qrelsFq.get(Integer.parseInt(idQuery[0].trim()));

                String fq = "id:" + getFq(ids);
                SolrDocumentList solrDocumentList = querySolr(idQuery[1].trim(), collection, zkHost, rows, fq);
                if (solrDocumentList != null)
                    appendResultsToFile(line.trim(), solrDocumentList.iterator(), outFile);

                StringBuilder missingDocs = new StringBuilder();
                //missingDocs.append(line).append("\n");
                missingDocs.append(getMissingDocs(solrDocumentList, ids, collection, zkHost));
                Utility.appendResultsToFile(missingDocs.toString(), missingDocsOutFile);

            }
        } catch (IOException e) {

        } finally {
            LineIterator.closeQuietly(lineIterator);
        }

    }

    String getFq(LinkedList<String> ids) {
        StringBuffer fqQuery = new StringBuffer();
        fqQuery.append("(");
        for (String str : ids) {
            fqQuery.append("\"").append(str).append("\"").append(" OR ");
        }
        fqQuery.append("id:0)");
        return fqQuery.toString();
    }

    String getMissingDocs(SolrDocumentList documentList, LinkedList<String> ids, String collection, String zkHost) {
        StringBuilder noContent = new StringBuilder();
        StringBuilder hasContent = new StringBuilder();
        documentList.forEach(doc -> {
            if (ids.contains(doc.get("id"))) {
                ids.remove(doc.get("id"));
            }

        });

      if(!ids.isEmpty()) {
          ids.forEach(id ->{
              String query = "id:\"" + id + "\"";
              SolrDocumentList doc = querySolr(query, collection, zkHost, 1, "");
              if(doc.get(0).get("content_t")!=null){
                  hasContent.append(doc.get(0).get("id")).append("\n");
              }      else{
                  noContent.append(doc.get(0).get("id")).append("\n");
              }
          });


           // ids.forEach(id -> noContent.append(id).append(" "));
        }
        //noContent.substring(0, noContent.lastIndexOf("\n"));
        String ret = noContent.toString() ;
                //"\n noContent \n" + noContent.toString();

        return ret.toString();
    }

    protected SolrDocumentList querySolr(String query, String collection, String zkHost, int rows, String fq) {
        SolrClient solrClient = SolrSupport.getCachedCloudClient(zkHost);
        SolrQuery solrQuery = new SolrQuery();
        solrQuery.set("collection", collection);
        solrQuery.setQuery(query);
        solrQuery.setRequestHandler("edismax");
        solrQuery.set("fl", "id, clusterId_i, score, content_t");
        solrQuery.setRows(rows);
        solrQuery.setFilterQueries(fq);
        int numRelCount = fq.split("OR").length-1;
        //System.out.println("numRelCount: " + numRelCount);
        //System.out.println(solrQuery.toQueryString());
        QueryResponse collResp = SolrQuerySupport.querySolr(solrClient, solrQuery, 0, null).get();
       // System.out.println(query);
        //System.out.println(collResp.getResults().getNumFound());
        return collResp.getResults();
    }

    protected SolrDocumentList querySolr(String query, String clusterColl, String statCollection,
                                         String zkHost, int rowsToRet, String fq, int numRowsCSIndx, int topShards) {

        Tuple2 response = reDDESelecitveSearch.relevantDDEBasedSelectiveSearch(zkHost, statCollection, clusterColl,
                query, numRowsCSIndx, topShards, rowsToRet, fq);
        if (response != null) {
            return response != null ? (SolrDocumentList) response._2() : null;
        } else {
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
            StringBuilder builder = new StringBuilder();
            builder.append(queryId.replace(":", ",")).append(",");
            // builder.append("\n");
            int[] array = new int[51];
            while (solrDocumentList.hasNext()) {
                SolrDocument document = solrDocumentList.next();
                int clusterId = Integer.parseInt(document.get("clusterId_i").toString());
                array[clusterId]++;
            }

            List<Integer> integersList = Ints.asList(array);
            Collections.sort(integersList, Collections.reverseOrder());

            for (Integer integer : integersList)
                if (integer > 0) {
                    builder.append(integer).append(",");
                }
           /*for(int i =1; i < array.length ; i++){
                if(array[i]>0)
                builder.append(i+ ": "+ array[i]).append(",");

            }*/
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
        HashMap<Integer, LinkedList<String>> qrelsFq = new HashMap<>();
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
                    if (docRel > 0)
                        docNumbers.add(qrel[2]);
                } else {
                    qrelsFq.put(number, docNumbers);
                    number++;
                    docNumbers = new LinkedList<>();
                    if (docRel > 0)
                        docNumbers.add(qrel[2]);

                }
            }

        } catch (IOException io) {

        }
        return qrelsFq;

    }

}
