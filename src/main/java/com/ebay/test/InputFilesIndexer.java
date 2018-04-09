package com.ebay.test;

import com.lucidworks.spark.util.SolrSupport;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import scala.collection.JavaConversions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Created by rajanishivarajmaski1 on 4/5/18.
 */
public class InputFilesIndexer {

    public static void main(String[] args) throws SolrServerException, IOException {
        String inputDirPath = "/Users/rajanishivarajmaski1/Desktop/ebay_test";
        InputFilesIndexer inputFilesIndexer = new InputFilesIndexer();
        inputFilesIndexer.parseFilesAndIndex(inputDirPath);
        System.out.println("end of program");
    }

    /**
     * Loop each file in directory -> loop through each line in file -> Index each record into solr
     *
     * @param inputDirPath
     * @throws IOException
     */
    public void parseFilesAndIndex(String inputDirPath) throws SolrServerException, IOException {
        File dir = new File(inputDirPath);
        System.out.println("Getting all files in " + dir.getCanonicalPath());
        List<File> files = (List<File>) FileUtils.listFiles(dir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
        for (File file : files) {
            System.out.println("file: " + file.getCanonicalPath());
            if(!file.isHidden()) {
                List<SolrInputDocument> docs = parseFileGetDocs(file.getCanonicalPath());
                indexToSolr("localhost:9983", "ebay", docs);
            }

        }
        System.out.println("done");
    }

    /**
     * Parse each line of file and obtain item id, title and generate solr input doc out of these key value pairs
     *
     * @param fileName
     * @return
     * @throws IOException
     */
    public List<SolrInputDocument> parseFileGetDocs(String fileName) throws IOException {
        List<SolrInputDocument> solrInputDocs = new LinkedList<>();

        //read file into stream, try-with-resources
        try (Stream<String> stream = Files.lines(Paths.get(fileName))) {

            stream.forEach(line -> solrInputDocs.add(getSolrInputDoc(line)));

        } catch (IOException e) {
            e.printStackTrace();
        }

        return solrInputDocs;
    }

    /**
     * Index batch of documents to solr
     *
     * @param zkHost
     * @param collectionName
     * @param solrInputDocs
     * @return
     */
    public int indexToSolr(String zkHost, String collectionName, List<SolrInputDocument> solrInputDocs) throws SolrServerException, IOException {
        SolrClient solrClient = SolrSupport.getCachedCloudClient(zkHost);
        SolrSupport.sendBatchToSolr(solrClient, collectionName, JavaConversions.collectionAsScalaIterable(solrInputDocs));
        solrClient.commit("ebay");
        return 0;
    }

    /**
     * Method to parse key value pairs and return solr input document object
     *
     * @param line : key value pairs, field name vs field value
     * @return : solr input document
     */
    public SolrInputDocument getSolrInputDoc(String line) {
        String[] tokens = line.split("\t");
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", tokens[0]);
        doc.addField("cid_s", tokens[2]);
        doc.addField("title_txt", tokens[3]);
        return doc;
    }

}
