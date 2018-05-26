package org.sfsu.cs.io.search.selectivesearch;

import org.apache.solr.common.SolrDocumentList;
import org.sfsu.cs.search.query.CORISelectiveSearchFileIndex;
import org.sfsu.cs.utils.Utility;

import java.util.List;

/**
 * Created by rajanishivarajmaski1 on 3/7/18.
 *
 * Selective Search algorithm that utilizes
 */
public class CoriSearchExecutor {

    public static void main(String[] args) {

        String coriStatsPath = args[0];///Users/rajanishivarajmaski1/University/csc895/selective-org.sfsu.cs.search/spark_cluster_job_op/20180326_152619 zucsoi localhost:9983 news_byte_d_idf
        String inputSearchFilePath = args[1]; //
        String zkHost = args[2];
        String collectionName = args[3];

        CORISelectiveSearchFileIndex coriSelectiveSearchFileIndex = new CORISelectiveSearchFileIndex();
        coriSelectiveSearchFileIndex.initCoriSelectiveSearch(coriStatsPath);
        String fieldToret =  "clusterId_s,score, content_t,id" ;
        List<String> searchQueries = getSearchQueries(inputSearchFilePath);
        for(String query : searchQueries) {
            SolrDocumentList solrDocumentList = coriSelectiveSearchFileIndex.executeCoriSelectiveSearch(zkHost, collectionName, query, 5, 0.4, fieldToret);
            writeResultsToFile(Utility.getFilePath(), query, solrDocumentList);
        }


    }

    private static void writeResultsToFile(String path, String query, SolrDocumentList solrDocuments){

    }

    private static List<String> getSearchQueries(String filePath){
        return null;
    }
}
