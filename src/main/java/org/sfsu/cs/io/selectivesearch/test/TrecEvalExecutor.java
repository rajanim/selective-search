package org.sfsu.cs.io.selectivesearch.test;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import java.io.*;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * A wrapper for trec_eval native binaries that is usable on a number
 * of platforms.
 * <br><b>Supported Platforms</b><br>
 * <ul>
 * <li>Windows (Intel 32bit)</li>
 * <li>Linux (Intel 32bit)</li>
 * <li>Mac OS X (Intel 64bit)</li>
 * </ul>
 *
 * @author Craig Macdonald
 */
public class TrecEvalExecutor {
    static boolean DELETE = true;
    static File trec_eval_temp = null;
    static Map<String, String[]> LIB_DEPENDENCIES = new HashMap<String, String[]>();

    static {
        LIB_DEPENDENCIES.put("trec_eval-win-x86", new String[]{"cygwin1.dll"});
        LIB_DEPENDENCIES.put("trec_eval-win-amd64", new String[]{"cygwin1.dll"});
    }

    ;

    File ourTE;
    int exit = Integer.MAX_VALUE;

    public TrecEvalExecutor() {
        ourTE = getTrecEvalBinary();
        //System.err.println(ourTE.toString());
        //System.err.println(ourTE.exists());
    }
    static String getOSShort() {
        String osName = System.getProperty("os.name");
        if (osName.startsWith("Windows"))
            return "win";
        if (osName.startsWith("Linux"))
            return "linux";
        if (osName.equals("Mac OS X"))
            return "macosx";
        throw new UnsupportedOperationException("Unsupported os: " + osName);
    }
    public static boolean isPlatformSupported() {
        return TrecEvalExecutor.class.getClassLoader().getResource(getExecName()) != null;
    }
    static File getTrecEvalBinary() {
        if (trec_eval_temp != null)
            return trec_eval_temp;
        final String resName = getExecName();
     /*   if (TrecEvalExecutor.class.getClassLoader().getResource(resName) == null)
            throw new UnsupportedOperationException("Unsupported os/arch: " + resName);*/

        File tempExec = null;
        try {
            Path tempExecDir = Files.createTempDirectory("jtrec_eval");
            if (DELETE)
                tempExecDir.toFile().deleteOnExit();

            tempExec = File.createTempFile("trec_eval", ".exe", tempExecDir.toFile());
            InputStream in = TrecEvalExecutor.class.getClassLoader().getResourceAsStream(resName);
            OutputStream out = new BufferedOutputStream(new FileOutputStream(tempExec));
            IOUtils.copy(in, out);
            in.close();
            out.close();
            tempExec.setExecutable(true);
            if (DELETE)
                tempExec.deleteOnExit();

            String[] libs = LIB_DEPENDENCIES.get(resName);
            if (libs != null)
                for (String lib : libs) {
                    File tmpLib = new File(tempExecDir.toFile(), lib);
                    in = TrecEvalExecutor.class.getClassLoader().getResourceAsStream(lib);
                    out = new BufferedOutputStream(
                            new FileOutputStream(tmpLib));
                    IOUtils.copy(in, out);
                    in.close();
                    out.close();
                    if (DELETE)
                        tmpLib.deleteOnExit();
                }

        } catch (Exception e) {
            throw new UnsupportedOperationException(e);
        }
        assert tempExec.exists() : "Exe file " + tempExec.toString() + " does not exist after creation";
        return tempExec;
    }
    protected static String getExecName() {
        return "trec_eval-" + getOSShort() + "-" + System.getProperty("os.arch");
    }
    /**
     * used for copying between streams
     */
    private static Thread inheritIO(final InputStream src, final PrintStream dest) {
        Thread t = new Thread(new Runnable() {
            public void run() {
                Scanner sc = new Scanner(src);
                while (sc.hasNextLine()) {
                    dest.println(sc.nextLine());
                }
                sc.close();
            }
        });
        t.start();
        return t;
    }
    /**
     * Directly invokes trec_eval
     *
     * @param args trec_eval commandline arguments
     */
    public static void main(String[] args) {

        String qrels = "/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/qrels_withDocName.txt";
        String res = "/Users/rajanishivarajmaski1/Desktop/selective_search/anagha/clueweb_queries/ExhaustiveSearchResultsFile_Clueweb_qrels.txt";
        TrecEvalExecutor te = new TrecEvalExecutor();
        String[][] output = te.runAndGetOutput(new String[]{ qrels, res});

        for (String[] strings : output) {
            for (String string : strings) {
                System.out.println( strings  + "  "+string);
            }
        }
        System.exit(new TrecEvalExecutor().run(args));
    }
    ProcessBuilder getBuilder(String[] args) {
        List<String> cmd = new ArrayList<String>();
        cmd.add(ourTE.getAbsolutePath().toString());
        for (String arg : args)
            cmd.add(arg);
        return new ProcessBuilder(cmd);
    }
    /**
     * Obtain the output from a trec_eval invocation
     *
     * @param args trec_eval commandline arguments
     * @return first dimension is for each line, second dimension is for each component
     */
    public String[][] runAndGetOutput(String[] args) {
        List<String[]> output = new ArrayList<String[]>();
        try {
            ProcessBuilder pb = getBuilder(args);
            pb.redirectError(Redirect.INHERIT);
            Process p = pb.start();
            InputStream in = p.getInputStream();
            LineIterator it = IOUtils.lineIterator(new InputStreamReader(in));
            while (it.hasNext()) {
                output.add(it.next().split("\\s+"));
            }
            p.waitFor();
            exit = p.exitValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (exit != 0)
            throw new RuntimeException("trec_eval ended with non-zero exit code (" + exit + ")");
        return output.toArray(new String[output.size()][]);
    }
    /**
     * @return Exit code of last invocation of trec_eval
     */
    public int getLastExitCode() {
        return exit;
    }
    /**
     * Invokes trec_eval and displays the output to this process's STDOUT stream.
     *
     * @param args trec_eval commandline arguments
     * @return exit code of trec_eval
     */
    public int run(String[] args) {
        try {
            ProcessBuilder pb = getBuilder(args);

            //see http://bugs.java.com/view_bug.do?bug_id=8023130
            final boolean windowsJVMRedirectBug = System.getProperty("java.specification.version").equals("1.7")
                    && System.getProperty("os.name").startsWith("Windows");

            if (!windowsJVMRedirectBug)
                pb.inheritIO();

            Thread t1 = null;
            Thread t2 = null;

            Process p = pb.start();
            if (windowsJVMRedirectBug) {
                //we dont need to redirect stdin, as trec_eval doesnt use it
                t1 = inheritIO(p.getInputStream(), System.out);
                t2 = inheritIO(p.getErrorStream(), System.err);
            }
            p.waitFor();
            exit = p.exitValue();
            //System.err.println(exit);
            if (windowsJVMRedirectBug) {
                assert t1 != null;
                assert t2 != null;
                t1.join();
                t2.join();
            }
        } catch (Exception e) {
            System.err.println(e);
            e.printStackTrace();
            exit = -1;
        }
        return exit;
    }
}