package org.sfsu.cs.io.search.selectivesearch;

import search.helper.CORIHelper;

/**
 * Created by rajanishivarajmaski1 on 3/7/18.
 * <p>
 * //Build shards statistics to execute selective search.
 */
public class CoriSearchStatsBuilder {

    public static void main(String[] args) {
        String zkHost = "localhost:9983";
        String clusterCollection = "NewsGroupImplicit";
        int numClusters = 20;
        CORIHelper coriHelper = new CORIHelper();

        coriHelper.logTermDFIndexCwAvgCwForCORI(clusterCollection, zkHost, numClusters);

    }
}
