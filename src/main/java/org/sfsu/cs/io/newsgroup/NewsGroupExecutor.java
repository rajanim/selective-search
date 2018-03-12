package org.sfsu.cs.io.newsgroup;

import com.lucidworks.spark.util.SolrSupport;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import scala.collection.JavaConversions;

import java.util.List;

/**
 * Created by rajanishivarajmaski1 on 2/21/18.
 */
public class NewsGroupExecutor {


    //1. Need to write method that will read cluster memberships o/p file generated from your clustering algorithm
    // and gets content of each doc.

    //2. Another method that passes key value pairs such as docId:<value>, clusterId:<value>, docContent : <value> to method getSolrInputDoc
    //List the input docs and pass onto method indexToSolr.



    /**
     * Index batch of documents to solr
     * @param zkHost
     * @param collectionName
     * @param solrInputDocs
     * @return
     */
    public int indexToSolr(String zkHost, String collectionName, List<SolrInputDocument> solrInputDocs) {
        SolrClient solrClient = SolrSupport.getCachedCloudClient(zkHost);
        SolrSupport.sendBatchToSolr(solrClient, collectionName, JavaConversions.collectionAsScalaIterable(solrInputDocs));
        return 0;
    }

    /**
     * Method to parse key value pairs and return solr input document object
     *
     * @param fields : key value pairs, field name vs field value
     * @return : solr input document
     */
    public SolrInputDocument getSolrInputDoc(String... fields) {
        SolrInputDocument doc = new SolrInputDocument(fields);
        return doc;
    }

}
