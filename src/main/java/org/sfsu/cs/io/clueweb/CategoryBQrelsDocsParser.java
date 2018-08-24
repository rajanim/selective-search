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
        categoryBQrelsDocsParser.parse("/Users/rajanishivarajmaski1/Downloads/categoryB_qrels_docs");

    }


    void parse(String path){
        LineIterator it = null;
        int count=0;
        try {
            it = FileUtils.lineIterator(new File(path), "UTF-8");
            StringBuilder builder = new StringBuilder();
            String rootPath = "/Users/rajanishivarajmaski1/ClueWeb09_English_9/qrels_docs_new/";
            String id="first";
            String prevLine="";
            while (it.hasNext()) {
                String line = it.nextLine();
                if (line.contains("EXTERNAL DOC ID:")) {
                    if(line.contains("clueweb09-en0012-20-34744")){
                        System.out.println("wait here");
                    }
                    int index = builder.lastIndexOf(prevLine);
                    builder.delete(index, builder.length());
                    writeToFile(builder.toString(), rootPath+id);
                    count+=1;
                    id = line.substring(line.indexOf(':') + 1).trim() ;
                    builder = new StringBuilder();
                    builder.append(line).append("\n");

                } else {
                    if(!line.trim().isEmpty()){
                        prevLine = line;
                        builder.append(line).append("\n");
                    }

                }

            }
        } catch (IOException ie) {




        } finally {
            LineIterator.closeQuietly(it);
            System.out.println("count: " + count);
        }

    }

    void parseDocs(String path) {
        LineIterator it = null;
        try {
            it = FileUtils.lineIterator(new File(path), "UTF-8");
            StringBuilder builder = new StringBuilder();
            String rootPath = "/Users/rajanishivarajmaski1/ClueWeb09_English_9/qrels_docs/";
            String fileName = "";
            String prevLine="";
            while (it.hasNext()) {
                String line = it.nextLine();
                if (line.contains("EXTERNAL DOC ID:")) {
                  // fileName  = rootPath + line.substring(line.indexOf(':') + 1).trim();
                    String[] recordLine = prevLine.split(" ");
                    fileName = rootPath+ recordLine[2].trim() ;
                    builder.append(line).append("\n");
                } else if (line.contains("</html>")) {
                    writeToFile(builder.toString(), fileName);
                    builder = new StringBuilder();
                } else {
                    if(!line.trim().isEmpty())
                    builder.append(line).append("\n");
                }
                prevLine = line;

            }
        } catch (IOException ie) {




        } finally {
            LineIterator.closeQuietly(it);
        }

    }
}
