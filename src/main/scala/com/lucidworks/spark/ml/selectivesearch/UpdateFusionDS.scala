  package com.lucidworks.spark.ml.selectivesearch


    import org.apache.http.client.methods.{HttpGet, HttpPost, HttpPut}
    import org.apache.http.entity.StringEntity
    import org.apache.http.impl.client.HttpClients
    import org.apache.http.util.EntityUtils

    import scala.io.Source

  /**
    * Created by rajanishivarajmaski1 on 1/3/19.
    *
    * Example scala script to update a data source
    */
  object UpdateFusionDS {


    val filename = "/Users/rajanishivarajmaski1/Desktop/urls.txt"
    val apiHost = "http://localhost:8765"
    val connectorsHost = "http://localhost:8984"
    val collection = "MISC"
    val indexPipeline = "MISC"
    val parser = "MISC"
    val dataSourceId = "misc"

    val bulkLinks = Source.fromFile(filename).getLines().mkString("\n")




    /*def main(args: Array[String]): Unit = {
      updateDataSource(dataSourceId, datasourceTemplate(dataSourceId, "www.google.com/"))
      //request("PUT", s"""$connectorsHost/connectors/v1/connectors/datasources/$dataSourceId""", datasourceTemplate(dataSourceId,bulkLinks))

    }*/

    /**
      *
      * @param method
      * @param url
      * @param jsonConfig
      * @return
      */
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
        println(String.valueOf(response.getStatusLine.getStatusCode))

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
  "created" : "2019-01-09T03:01:22.953Z",
  "modified" : "2019-01-09T03:01:22.953Z",
  "connector" : "lucid.web",
  "type" : "web",
  "pipeline" : "MISC",
  "parserId" : "MISC",
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
    "f.extraLoadTimeMs" : 250,
    "f.jsPageLoadTimeout" : 20000,
    "restrictToTreeAllowSubdomains" : false,
    "f.jsScriptTimeout" : 20000,
    "f.requestCounterMinWaitMs" : 5000,
    "maxItems" : -1,
    "f.jsAjaxTimeout" : 20000,
    "trackEmbeddedIDs" : true,
    "dedupe" : false,
    "f.scrapeLinksBeforeFiltering" : false,
    "f.allowAllCertificates" : false,
    "collection" : "MISC",
    "forceRefresh" : false,
    "delete404" : true,
    "f.obeyRobots" : true,
    "parserRetryCount" : 0,
    "f.quitTimeoutMs" : 5000,
    "fetchDelayMSPerHost" : true,
    "fetchThreads" : 5,
    "indexCrawlDBToSolr" : false,
    "f.requestRetryCount" : 0,
    "retainOutlinks" : false,
    "restrictToTree" : true,
    "f.defaultCharSet" : "UTF-8",
    "f.useRequestCounter" : true,
    "emitThreads" : 5,
    "f.headlessBrowser" : true,
    "f.useFirefox" : false,
    "f.canonicalTagsRedirectLimit" : 4,
    "diagnosticMode" : false,
    "f.requestCounterMaxWaitMs" : 20000,
    "f.followCanonicalTags" : true,
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
      "id" : "Anda",
      "type" : "field-mapping"
    },
    "f.extraPageLoadDeltaChars" : 0,
    "restrictToTreeUseHostAndPath" : false,
    "sitemap_incremental_crawling" : false,
    "f.screenshotFullscreen" : false,
    "f.filteringRootTags" : [ "body", "head" ],
    "startLinks" : [ "http://somethingelse.com" ],
    "failFastOnStartLinkFailure" : true,
    "f.timeoutMS" : 10000,
    "f.discardLinkURLAnchors" : true,
    "chunkSize" : 1,
    "f.simulateMobile" : false,
    "f.obeyRobotsDelay" : true,
    "f.useHighPerfJsEval" : false,
    "deleteErrorsAfter" : -1,
    "f.userAgentName" : "Lucidworks-Anda/2.0",
    "retryEmit" : true,
    "f.crawlJS" : false,
    "depth" : -1,
    "f.cookieSpec" : "browser-compatibility",
    "refreshStartLinks" : false,
    "f.maxSizeBytes" : 4194304,
    "aliasExpiration" : 1,
    "f.takeScreenshot" : false
  }
}""".replaceAll("\n", "")

    request("PUT", s"""$connectorsHost/connectors/v1/connectors/datasources/$dataSourceId""", datasourceTemplate(dataSourceId,bulkLinks))



  }

