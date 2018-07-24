package org.sfsu.cs.io.bbc;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by rajanishivarajmaski1 on 7/21/18.
 */
public class RenameBBCFiles {

    public static void main(String[] args) throws IOException{
        RenameBBCFiles renameBBCFiles = new RenameBBCFiles();
        renameBBCFiles.renameMoveFiles("/Users/rajanishivarajmaski1/University/bbc_dataset/bbcsport/","/Users/rajanishivarajmaski1/University/bbc_dataset/bbcsport_all/");
    }

    void renameMoveFiles(String bbcDatasetPath, String moveToDir) throws IOException{

        File destDir = new File(moveToDir);
        Iterator<File> fileIterator = FileUtils.iterateFilesAndDirs
                (new File(bbcDatasetPath), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);

        while (fileIterator.hasNext()){
            File srcFile = fileIterator.next();
            if(srcFile.isFile()) {
                if(srcFile.getName().contains(".DS_Store")){
                    continue;
                }
                String fileName = srcFile.getParent().replace("/", "_") + "_" + srcFile.getName();
                File destFile = new File(moveToDir + fileName);
                srcFile.renameTo(destFile);

                //FileUtils.copyFileToDirectory(sr, destDir, false);
            }
        }

    }
}
