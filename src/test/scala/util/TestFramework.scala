package util

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.eclipse.jetty.servlet.ServletHolder
import org.junit.Assert._
import org.restlet.ext.servlet.ServerServlet
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.apache.solr.cloud.MiniSolrCloudCluster

trait SolrCloudTestBuilder extends BeforeAndAfterAll with LazyLogging {
  this: Suite =>

  @transient var cluster: MiniSolrCloudCluster = _
  @transient var cloudClient: CloudSolrClient = _
  var zkHost: String = _
  var testWorkingDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val solrXml = new File("src/test/resources/solr.xml")
    val solrXmlContents: String = org.sfsu.cs.TestSolrCloudClusterSupport.readSolrXml(solrXml)
    val targetDir = new File("target")
    if (!targetDir.isDirectory)
      fail("Project 'target' directory not found at :" + targetDir.getAbsolutePath)

    testWorkingDir = new File(targetDir, "scala-solrcloud-" + System.currentTimeMillis)
    if (!testWorkingDir.isDirectory)
      testWorkingDir.mkdirs

    // need the schema stuff
    val extraServlets: java.util.SortedMap[ServletHolder, String] = new java.util.TreeMap[ServletHolder, String]()

    val solrSchemaRestApi: ServletHolder = new ServletHolder("SolrSchemaRestApi", classOf[ServerServlet])
    solrSchemaRestApi.setInitParameter("org.restlet.application", "org.apache.solr.rest.SolrSchemaRestApi")
    extraServlets.put(solrSchemaRestApi, "/schema/*")

    cluster = new MiniSolrCloudCluster(1, null /* hostContext */ ,
      testWorkingDir.toPath, solrXmlContents, extraServlets, null /* extra filters */)
    cloudClient = new CloudSolrClient(cluster.getZkServer.getZkAddress, true)
    cloudClient.connect()

    assertTrue(!cloudClient.getZkStateReader.getClusterState.getLiveNodes.isEmpty)
    zkHost = cluster.getZkServer.getZkAddress
  }

  override def afterAll(): Unit = {
    cloudClient.close()
    cluster.shutdown()

    if (testWorkingDir != null && testWorkingDir.isDirectory) {
      FileUtils.deleteDirectory(testWorkingDir)
    }

    super.afterAll()
  }

}

trait SparkSolrContextBuilder extends BeforeAndAfterAll {
  this: Suite =>

  @transient var sparkSession: SparkSession = _
  @transient var sc: SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .appName("spark-solr-tester")
      .master("local")
      .config("spark.default.parallelism", "1")
      .getOrCreate()

    sc = sparkSession.sparkContext
  }

  override def afterAll(): Unit = {
    sparkSession.stop()
    super.afterAll()
  }
}

// General builder to be used by all the tests that need Solr and Spark running
trait TestSuiteBuilder extends SparkSolrFunSuite with SparkSolrContextBuilder with SolrCloudTestBuilder {}





