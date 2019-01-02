package org.sfsu.cs.io.clueweb;

import org.apache.commons.io.IOUtils;
import org.jwat.common.HeaderLine;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

import java.io.*;
import java.util.Iterator;

/**
 * An iterator over a WARC file that returns WarcEntry objects.
 * <p>
 * This iterator discards records that are not responses or that do not have
 * the WARC-TREC-ID header.
 */
public class ResponseIterator extends AbstractIterator<ResponseIterator.WarcEntry> implements Serializable {

    /**
     * Name of the header with the Trec identifier.
     */
    final public static String TREC_ID = "WARC-TREC-ID";
    final WarcReader reader;
    public Iterator<WarcRecord> getIter() {
        return iter;
    }
    final Iterator<WarcRecord> iter;
    private final InputStream input;
    private int errors = 0;
    /**
     * Record offset
     */
    private int n = 0;

    public ResponseIterator(FileInputStream input) throws IOException {
        this.input = input;
        //this.reader = WarcReaderFactory.getReader(new GZIPInputStream(input));
        this.reader = WarcReaderFactory.getReader((input));

        this.iter = reader.iterator();

    }


    public ResponseIterator(InputStream input) throws IOException {
        this.input = input;
        this.reader = WarcReaderFactory.getReader(input);

        this.iter = reader.iterator();
       /* if(iter.hasNext())
            System.out.println("has next here");

        iter.next().getHeaderList().forEach(value -> System.out.println(value.name));
*/
    }
    public static boolean isResponse(WarcRecord record) {
        HeaderLine typeHeader = record.getHeader("WARC-Type");
        if (typeHeader != null) {
            return typeHeader.value.equals("response");
        }
        return false;
    }
    public int errors() {
        return errors;
    }
    @Override
    public WarcEntry computeNext() {
        while (iter.hasNext()) {
            WarcRecord record = iter.next();
            n++;
            // Skip records that are not responses
          /*  if (!isResponse(record)){
                continue;
            }*/

            HeaderLine idHeader = record.getHeader(TREC_ID);
            if (idHeader == null) {
                errors += 1;
                continue;
            }
            String trecId = idHeader.value;
            int offset = record.header.headerBytes.length;

            String contentType = (record.getHttpHeader() != null) ? record.getHttpHeader().contentType : null;
            String warcType = record.getHeader("WARC-Type").value;

            try {
                byte[] httpHeader = record.getHttpHeader().getHeader();
                byte[] content = IOUtils.toByteArray(record.getPayloadContent());
                return new WarcEntry(trecId, content, httpHeader, offset, contentType,warcType);
            } catch (Exception e) {
                System.err.printf("Error reading record %d: %s", n, e);
                errors += 1;
            }
        }
        try {
            reader.close();
            input.close();
        } catch (Exception e) {
            // Ignore
        }
        return endOfData();
    }
    public void close() throws IOException {
        this.reader.close();
        if(input!=null)
        this.input.close();
    }

    /**
     * (Oh, Java, why you don't have tuples?)
     */
    public static class WarcEntry implements Serializable {
        public final byte[] content;
        public final String trecId;
        public final int contentOffset;
        public final String contentType;
        public final byte[] httpHeader;
        public final String warcType;

        public WarcEntry(String trecId, byte[] content, byte[] httpHeader, int contentOffset, String contentType, String warcType) {
            this.trecId = trecId;
            this.content = content;
            this.httpHeader = httpHeader;
            this.contentOffset = contentOffset;
            this.contentType = contentType;
            this.warcType = warcType;
        }
    }

}
