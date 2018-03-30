package org.sfsu.cs.io.search.selectivesearch;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import search.query.CORISelectiveSearch;

/**
 * Created by rajanishivarajmaski1 on 3/7/18.
 *
 * Selective Search algorithm that utilizes
 */
public class CoriSearchExecutor {

    public static void main(String[] args) {

        String coriStatsPath = args[0];///Users/rajanishivarajmaski1/University/csc895/selective-search/spark_cluster_job_op/20180326_152619 zucsoi localhost:9983 news_byte_d_idf
        String searchTerm = args[1]; //
        String zkHost = args[2];
        String collectionName = args[3];

        CORISelectiveSearch coriSelectiveSearch = new CORISelectiveSearch();
        coriSelectiveSearch.initCoriSelectiveSearch(coriStatsPath);
        SolrDocumentList solrDocumentList = coriSelectiveSearch.executeCoriSelectiveSearch(zkHost,collectionName, searchTerm, 0.4);
       for(SolrDocument doc : solrDocumentList){
           System.out.println(doc.toString());
       }



    }
}
