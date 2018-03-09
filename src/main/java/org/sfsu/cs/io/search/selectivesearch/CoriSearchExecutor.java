package org.sfsu.cs.io.search.selectivesearch;

import search.query.CORISelectiveSearch;

/**
 * Created by rajanishivarajmaski1 on 3/7/18.
 *
 * Selective Search algorithm that utilizes
 */
public class CoriSearchExecutor {

    public static void main(String[] args) {

        String coriStatsPath = args[0];
        //String coriStatsPath = "/Users/rajanishivarajmaski1/University/spark-solr-899/spark-solr/spark_cluster_job_op/20171023_134721";
        CORISelectiveSearch coriSelectiveSearch = new CORISelectiveSearch();
        coriSelectiveSearch.initCoriSelectiveSearch(coriStatsPath);
        coriSelectiveSearch.executeCoriSelectiveSearch("localhost:9983", "Test", "solr", 0.4);

    }
}
