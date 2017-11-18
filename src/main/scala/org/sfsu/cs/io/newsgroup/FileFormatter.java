package org.sfsu.cs.io.newsgroup;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;

/**
 * Flatten a file into format that can be read by the Bayes M/R job.
 * Forked from Tamming text and Mahout libraries https://github.com/tamingtext/book/tree/master/src/main/java/com/tamingtext/classifier
 * <p/>
 * One document per line, first token is the label followed by a tab, rest of the line are the terms.
 */
public final class FileFormatter {

    private static final Logger log = LoggerFactory.getLogger(FileFormatter.class);

    private FileFormatter() {
    }
    

    /**
     * Write the input files to the outdir, one output file per input file
     *
     * @param label    The label of the file
     * @param analyzer The analyzer to use
     * @param input    The input file or directory. May not be null
     * @param charset  The Character set of the input files
     * @param outDir   The output directory. Files will be written there with the same name as the input file
     */
    public static void format(String label, Analyzer analyzer, File input,
                              Charset charset, File outDir) throws IOException {
        if (input.isDirectory()) {
            input.listFiles(new FileProcessor(label, analyzer, charset, outDir));
        } else {
            Writer writer = Files.newWriter(new File(outDir, input.getName()), charset);
            try {
                writeFile(label, analyzer, input, charset, writer);
            } finally {
                Closeables.closeQuietly(writer);
            }
        }
    }
    /**
     * Write the tokens and the label from the Reader to the writer
     *
     * @param label    The label
     * @param analyzer The analyzer to use
     * @param inFile   the file to read and whose contents are passed to the analyzer
     * @param charset  character encoding to assume when reading the input file
     * @param writer   The Writer, is not closed by this method
     * @throws IOException if there was a problem w/ the reader
     */
    private static void writeFile(String label, Analyzer analyzer, File inFile,
                                  Charset charset, Writer writer) throws IOException {
        Reader reader = Files.newReader(inFile, charset);
        try {
            //TokenStream ts = analyzer.reusableTokenStream(label, reader);
            TokenStream ts = analyzer.tokenStream(label, reader);

            writer.write(label);
            writer.write('\t'); // edit: Inorder to match Hadoop standard
            // TextInputFormat
            CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            while (ts.incrementToken()) {
                char[] termBuffer = termAtt.buffer();
                int termLen = termAtt.length();
                writer.write(termBuffer, 0, termLen);
                writer.write(' ');
            }
        } finally {
            Closeables.closeQuietly(reader);
        }
    }

    /**
     * Hack the FileFilter mechanism so that we don't get stuck on large directories and
     * don't have to loop the list twice
     */
    private static final class FileProcessor implements FileFilter {
        private final String label;

        private final Analyzer analyzer;
        private final Charset charset;
        private File outputDir;
        private Writer writer;

        /**
         * Use this when you want to collapse all files to a single file
         *
         * @param label  The label
         * @param writer must not be null and will not be closed
         */
        private FileProcessor(String label, Analyzer analyzer, Charset charset, Writer writer) {
            this.label = label;
            this.analyzer = analyzer;
            this.charset = charset;
            this.writer = writer;
        }

        /**
         * Use this when you want a writer per file
         *
         * @param outputDir must not be null.
         */
        private FileProcessor(String label, Analyzer analyzer, Charset charset, File outputDir) {
            this.label = label;
            this.analyzer = analyzer;
            this.charset = charset;
            this.outputDir = outputDir;
        }

        @Override
        public boolean accept(File file) {
            if (file.isFile()) {
                Writer theWriter = null;
                try {
                    if (writer == null) {
                        theWriter = Files.newWriter(new File(outputDir, file.getName()), charset);
                    } else {
                        theWriter = writer;
                    }
                    writeFile(label, analyzer, file, charset, theWriter);
                    if (writer != null) {
                        // just write a new line
                        theWriter.write('\n');
                    }
                } catch (IOException e) {
                    // TODO: report failed files instead of throwing exception
                    throw new IllegalStateException(e);
                } finally {
                    if (writer == null) {
                        Closeables.closeQuietly(theWriter);
                    }
                }
            } else {
                file.listFiles(this);
            }
            return false;
        }
    }
}
