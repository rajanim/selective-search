package org.sfsu.cs.io.search.selectivesearch;

import org.sfsu.cs.search.helper.CORIHelper;

/**
 * Created by rajanishivarajmaski1 on 3/7/18.
 * <p>
 * //Build shards statistics to execute selective org.sfsu.cs.search.
 */
public class CoriSearchStatsBuilder {

    public static void main(String[] args) {
        String zkHost = "localhost:9983";
        String clusterCollection = "clueweb_s";
        int numClusters = 50;
        CORIHelper coriHelper = new CORIHelper();

        //coriHelper.logTermDFIndexCwAvgCwForCORI(clusterCollection, zkHost, numClusters);
        coriHelper.indexTermDFIndexCwAvgCwForCORI("clueweb_qrels_cori", clusterCollection, zkHost, numClusters);

    }
}
