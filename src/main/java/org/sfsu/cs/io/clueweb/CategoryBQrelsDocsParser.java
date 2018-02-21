package org.sfsu.cs.io.clueweb;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.IOException;

import static ucar.nc2.util.IO.writeToFile;

/**
 * Created by rajanishivarajmaski1 on 2/12/18.
 */
public class CategoryBQrelsDocsParser {

    public static void main(String[] args) {
        CategoryBQrelsDocsParser categoryBQrelsDocsParser = new CategoryBQrelsDocsParser();
        categoryBQrelsDocsParser.parseDocs("/Users/rajanishivarajmaski1/Downloads/categoryB_qrels_docs");

    }

    void parseDocs(String path) {
        LineIterator it = null;
        try {
            it = FileUtils.lineIterator(new File(path), "UTF-8");
            StringBuilder builder = new StringBuilder();
            String rootPath = "/Users/rajanishivarajmaski1/ClueWeb09_English_9/qrels_docs/";
            String fileName = "";
            while (it.hasNext()) {
                String line = it.nextLine();
                if (line.contains("EXTERNAL DOC ID:")) {
                    fileName = rootPath + line.substring(line.indexOf(':') + 1);
                } else if (line.equals("</html>")) {
                    writeToFile(builder.toString(), fileName.trim() + ".html");
                    builder = new StringBuilder();
                } else {
                    builder.append(line);
                }

            }
        } catch (IOException ie) {

        } finally {
            LineIterator.closeQuietly(it);
        }

    }
}
