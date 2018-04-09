package org.sfsu.cs.io.newsgroup;

import com.lucidworks.spark.util.SolrSupport;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import scala.collection.JavaConversions;

import java.util.List;
import java.lang.*;
import java.io.*;
import java.util.*;


/**
 * Created by rajanishivarajmaski1 on 2/21/18.
 * Edited by nikhilparatkar on 03/01/18
 */
class Document
{
        int docId;
        String docDirectory;
        int clusterId;
        public Document(int docId,String docDirectory,int clusterId)
        {
                this.docId = docId;
                this.clusterId = clusterId;
                this.docDirectory = docDirectory;
        }
            @Override 
        public String toString()
        {
                return String.format("|DocId:"+this.docId+" ClusterId:"+this.clusterId);
        }
}
class FileSearch
{
        String fileNameToSearch;
        //ArrayList<String> result = new ArrayList<>();
        String result;
        public void searchDirectory(File directory,String fileNameToSearch)
        {
                this.fileNameToSearch = fileNameToSearch;
                if(directory.isDirectory())
                {
                        search(directory);
                }
                else
                        System.out.println(directory.getAbsoluteFile());
        }
            public void search(File file)
        {
                if(file.canRead())
                {
                        for(File tmp:file.listFiles())
                        {
                                if(tmp.isDirectory())
                                        search(tmp);
                                else if(fileNameToSearch.equals(tmp.getName().toLowerCase()))
                                {
                                        //result.add(tmp.getAbsoluteFile().toString());
                                        result = tmp.getAbsoluteFile().toString();
                                }

                        }
                }
                else
                    System.out.println(file.getAbsoluteFile()+"  - Permission Denied!");
        }
}
public class NewsGroupExecutor {

    String outputFilePath;
    String algorithm;
    String collectionPath;

    //1. Need to write method that will read cluster memberships o/p file generated from your clustering algorithm
    // and gets content of each doc.

    //2. Another method that passes key value pairs such as docId:<value>, clusterId:<value>, docContent : <value> to method getSolrInputDoc
    //List the input docs and pass onto method indexToSolr.

    public static void main(String[] args)
    {
        NewsGroupExecutor obj = new NewsGroupExecutor();
        System.out.println("Calling prepareDocs");
        //Read input params here, pass them to prepareDocs
        obj.readParameters();
        obj.prepareDocs();
    }

    public void readParameters()
    {
        try{
                Properties prop = new Properties();
                InputStream inputStream = getClass().getClassLoader().getResourceAsStream("config.properties");
                if (inputStream != null) {
                         prop.load(inputStream);
                } else {
                        throw new FileNotFoundException("property file 'config.properties' not found in the classpath");
                }
                outputFilePath = prop.getProperty("outputFilePath");
                algorithm = prop.getProperty("algorithm");
                collectionPath  = prop.getProperty("collectionPath");
        }catch(Exception ex){
                System.out.println("Exception: " + ex);
        }

    }
	
    public void prepareDocs()
    {
        String line;
        int clusterId=999;
        ArrayList<Document> documents = new ArrayList<>();
        ArrayList<SolrInputDocument> solrInputDocs = new ArrayList<>();
        try
        {
        FileReader fileReader = new FileReader(outputFilePath);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        if(algorithm.equals("Kmeans"))
        {

        }
        else if(algorithm.equals("LDA"))
        {
            System.out.println("Reading LDA Results");
            while((line = bufferedReader.readLine())!=null)
            {
                //System.out.println(line);
                if(line.toLowerCase().startsWith("cluster"))
                {
                    //Read cluster ID
                    String[] tokens = line.split(" ");
                    clusterId = Integer.parseInt(tokens[1].split(":")[0]);
                    //System.out.println("Cluster:"+clusterId);
                }
                else if(line.toLowerCase().startsWith("doc count"))
                {
                    //System.out.println(line);
                    //Read doc Cunt
                }
                else if(line.toLowerCase().startsWith("document ids"))
                {
                    //System.out.println(line);
                    line = bufferedReader.readLine();
                    if(line!=null)
                    {
                        //Read Doc IDs
			String[] docIds = line.split(" ");
                        for(String id:docIds)
                        {
                            if(!id.equals(" ")&&id.length()!=0)
                            {
                                String[] details = id.split("/");
                                Document document = new Document(Integer.parseInt(details[1].trim()),details[0].trim(),clusterId);
                                documents.add(document);
                            }
                        }
                    }
                }

            }
        }
        bufferedReader.close();
        System.out.println("Result file parsed.");
        }
        catch(Exception ex)
        {
            System.out.println("!Exception ocured:"+ex.toString());
        }
	
	/**
        * READ DOCUMENT CONTENTS
        */
        System.out.println("Fetching documents");
        for(Document d:documents)
        {
            FileSearch fs = new FileSearch();
            fs.searchDirectory(new File(collectionPath+d.docDirectory),Integer.toString(d.docId));
                StringBuilder sb = new StringBuilder();
            //System.out.println("Document "+d.docDirectory+"/"+d.docId+" found in:"+fs.result);
            try
            {
                FileReader fr = new FileReader(fs.result);
                BufferedReader br = new BufferedReader(fr);

                while((line = br.readLine())!=null)
                {
                    sb.append(line);
                    sb.append("\n");
                }
                br.close();
            }
            catch(Exception ex)
            {
                System.out.println("!Exception ocured:"+ex.toString());
            }
            String[] inputString ={"id",Integer.toString(d.docId),"clusterId_i",Integer.toString(d.clusterId+1),"docContent_t",sb.toString(),"route","shard"+Integer.toString(d.clusterId+1)};
            System.out.println("Getting solr doc:"+inputString[1]+"\n");
            solrInputDocs.add(getSolrInputDoc(inputString));
        }

        //HERE, call indexToSolr method
        indexToSolr("localhost:9983","NewsGroupImplicit",solrInputDocs);
    }


    /**
     * Index batch of documents to solr
     * @param zkHost
     * @param collectionName
     * @param solrInputDocs
     * @return
     */
    public int indexToSolr(String zkHost, String collectionName, List<SolrInputDocument> solrInputDocs) {
	System.out.println("Calling indexToSolr method\nString zkHost: "+zkHost);
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
