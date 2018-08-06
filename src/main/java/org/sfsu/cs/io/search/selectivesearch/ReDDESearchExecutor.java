package org.sfsu.cs.io.search.selectivesearch;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.sfsu.cs.search.query.ReDDESelecitveSearch;
import scala.Tuple2;

/**
 * Created by rajanishivarajmaski1 on 5/16/18.
 */
public class ReDDESearchExecutor {

    public static void main(String[] args) {
        ReDDESelecitveSearch reDDESelecitveSearch = new ReDDESelecitveSearch();
        Tuple2 response = reDDESelecitveSearch.relevantDDEBasedSelectiveSearch("localhost:9983", "clueweb_redde", "clueweb", "obama family tree", 10, 75, 100,"");
        SolrDocumentList documentList = (SolrDocumentList) response._2();
        for (SolrDocument doc : documentList) {
            System.out.println(doc.get("id"));
        }
    }
}
