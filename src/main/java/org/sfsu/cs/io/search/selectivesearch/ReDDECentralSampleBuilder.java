package org.sfsu.cs.io.search.selectivesearch;

import org.sfsu.cs.search.helper.ReDDEHelper;

/**
 * Created by rajanishivarajmaski1 on 5/16/18.
 */
public class ReDDECentralSampleBuilder {

    public static void main(String[] args) {
        ReDDEHelper helper = new ReDDEHelper();
        helper.buildCSIndex("clueweb", "localhost:9983",75,"clueweb_redde",10);

    }
}
