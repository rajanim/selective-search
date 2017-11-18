package org.sfsu.cs.io.newsgroup;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.Charset;

/**
 *
 * Forked from Tamming text and Mahout libraries https://github.com/tamingtext/book/tree/master/src/main/java/com/tamingtext/classifier
 * Prepare the 20 Newsgroups files for training using the
 * <p>
 * This class takes the directory containing the extracted newsgroups and collapses them into a single file
 * per category, with one document per line (first token on each line is the label)
 */
public final class PrepareTwentyNewsgroups {

    private PrepareTwentyNewsgroups() {
    }

    public static void main(String[] args) throws Exception {

        File parentDir = new File(("/Users/rajanishivarajmaski1/University/CSC849_Search/20news-bydate/20news-18828"));
        File outputDir = new File(("/Users/rajanishivarajmaski1/University/CSC849_Search/20news-bydate/20news-18828_in_1"));
        // String analyzerName = (String) cmdLine.getValue(analyzerNameOpt);
        Charset charset = Charset.forName(("utf-8"));
        Analyzer analyzer = new StandardAnalyzer();
        // parent dir contains dir by category
        if (!parentDir.exists()) {
            throw new FileNotFoundException("Can't find input directory " + parentDir);
        }
        File[] categoryDirs = parentDir.listFiles();
        for (File dir : categoryDirs) {
            if (dir.isDirectory()) {
                if (!outputDir.exists() && !outputDir.mkdirs()) {
                    throw new IllegalStateException("Can't create output directory");
                }

                // File outputFile = new File(outputDir, dir.getName() + ".txt");
                FileFormatter.format(dir.getName(), analyzer, dir, charset, outputDir);
            }
        }
    }
}