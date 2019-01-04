  package com.lucidworks.spark.ml.selectivesearch


  import org.apache.http.client.methods.{HttpGet, HttpPost, HttpPut}
  import org.apache.http.entity.StringEntity
  import org.apache.http.impl.client.HttpClients
  import org.apache.http.util.EntityUtils

  import scala.io.Source

  /**
    * Created by rajanishivarajmaski1 on 1/3/19.
    */
  object UpdateFusionDS {


    val filename = "/d/d1/lw/fusion/4.1.0/sharepoint_urls.txt"
    val apiHost = "http://localhost:8765"
    val connectorsHost = "http://localhost:8984"
    val collection = "MorganStanleyEUTNBTO"
    val indexPipeline = "sharepoint"
    val parser = "sharepoint"
    val dataSourceId = "SharePoint"

    val bulkLinks = Source.fromFile(filename).getLines().mkString("\n")



    def main(args: Array[String]): Unit = {
      request("PUT", s"""$apiHost/api/v1/jobs/datasource:${dataSourceId}""", datasourceTemplate(dataSourceId, bulkLinks))

    }

    def request(method: String, url: String, jsonConfig: String = null): Boolean = {
      val client = HttpClients.createDefault()
      try {
        val client = HttpClients.createDefault()

        val httpCall = method match {
          case "GET" => new HttpGet(url)
          case _ =>
            val call = method match {
              case "POST" => new HttpPost(url)
              case "PUT" => new HttpPut(url)
            }
            call.setEntity(new StringEntity(jsonConfig))
            call.setHeader("Accept", "application/json")
            call.setHeader("Content-type", "application/json")
            call
        }

        val response = client.execute(httpCall)

        EntityUtils.consume(response.getEntity)
        // check that response is a 200 response
        response.getStatusLine.getStatusCode == 200
      } finally {
        client.close()
      }
    }

    def updateDataSource(id: String, jsonConfig: String): Boolean = {
      request("PUT", s"""$connectorsHost/connectors/v1/connectors/datasources/$id""", jsonConfig)
    }

    val datasourceTemplate = (id: String, links: String) =>
      s"""{
  "id" : "$id",
  "connector" : "lucid.web",
  "type" : "web",
  "pipeline" : "$indexPipeline",
  "parserId" : "$parser",
  "properties" : {
    "refreshOlderThan" : -1,
    "f.appendTrailingSlashToLinks" : false,
    "refreshErrors" : false,
    "restrictToTreeIgnoredHostPrefixes" : [ "www." ],
    "dedupeSaveSignature" : false,
    "crawlDBType" : "in-memory",
    "f.firefoxHeadlessBrowser" : true,
    "f.discardLinkURLQueries" : false,
    "fetchDelayMS" : 0,
    "f.respectMetaEquivRedirects" : false,
    "refreshAll" : true,
    "f.defaultMIMEType" : "application/octet-stream",
    "f.extraLoadTimeMs" : 0,
    "f.jsPageLoadTimeout" : 20000,
    "restrictToTreeAllowSubdomains" : false,
    "f.jsScriptTimeout" : 20000,
    "maxItems" : -1,
    "f.jsAjaxTimeout" : 20000,
    "dedupe" : false,
    "f.scrapeLinksBeforeFiltering" : false,
    "trackEmbeddedIDs" : true,
    "f.allowAllCertificates" : false,
    "collection" : "$collection",
    "forceRefresh" : false,
    "f.obeyRobots" : true,
    "parserRetryCount" : 0,
    "fetchDelayMSPerHost" : true,
    "fetchThreads" : 5,
    "indexCrawlDBToSolr" : false,
    "f.requestRetryCount" : 0,
    "restrictToTree" : true,
    "retainOutlinks" : false,
    "f.defaultCharSet" : "UTF-8",
    "emitThreads" : 5,
    "f.useFirefox" : true,
    "diagnosticMode" : false,
    "delete" : true,
    "initial_mapping" : {
      "mappings" : [ {
        "source" : "charSet",
        "operation" : "move",
        "target" : "charSet_s"
      }, {
        "source" : "fetchedDate",
        "operation" : "move",
        "target" : "fetchedDate_dt"
      }, {
        "source" : "lastModified",
        "operation" : "move",
        "target" : "lastModified_dt"
      }, {
        "source" : "signature",
        "operation" : "move",
        "target" : "dedupeSignature_s"
      }, {
        "source" : "contentSignature",
        "operation" : "move",
        "target" : "signature_s"
      }, {
        "source" : "length",
        "operation" : "move",
        "target" : "length_l"
      }, {
        "source" : "mimeType",
        "operation" : "move",
        "target" : "mimeType_s"
      }, {
        "source" : "parent",
        "operation" : "move",
        "target" : "parent_s"
      }, {
        "source" : "owner",
        "operation" : "move",
        "target" : "owner_s"
      }, {
        "source" : "group",
        "operation" : "move",
        "target" : "group_s"
      } ],
      "reservedFieldsMappingAllowed" : false,
      "skip" : false,
      "id" : "FromMap",
      "type" : "field-mapping"
    },
    "f.extraPageLoadDeltaChars" : 0,
    "restrictToTreeUseHostAndPath" : false,
    "f.filteringRootTags" : [ "body", "head" ],
    "startLinks" : [ ${links.map(link => s""""$link"""").mkString(",")} ],
    "failFastOnStartLinkFailure" : true,
    "f.timeoutMS" : 10000,
    "f.discardLinkURLAnchors" : true,
    "includeRegexes" : [ ${links.map(link => s""""$link.*"""").mkString(",")} ],
    "chunkSize" : 1,
    "f.obeyRobotsDelay" : true,
    "deleteErrorsAfter" : -1,
    "f.userAgentName" : "Lucidworks-Anda/2.0",
    "retryEmit" : true,
    "f.crawlJS" : false,
    "depth" : -1,
    "f.cookieSpec" : "browser-compatibility",
    "f.maxSizeBytes" : 4194304,
    "refreshStartLinks" : false,
    "aliasExpiration" : 1
  },
  "licensed" : true
  }""".replaceAll("\n", "")




  }

