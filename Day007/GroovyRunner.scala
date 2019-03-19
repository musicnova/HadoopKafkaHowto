package ru.croc.smartdata.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

/**
 *
 */
public class GroovyRunner {

    /**
     * Load script by path and process it like it would be processed in Prediction Engine
     * For usage in testing or from console
     * @param scriptName
     * @param scriptParams
     * @return Integer
     * @throws Exception
     */
    public static int loadAndProcessScript(String scriptName, String path, Object scriptParams) throws Exception {
        GroovyPredictor predictor = new GroovyPredictor(scriptName);
        return predictor.process(loadLocalFile(path), scriptParams);
    }

    /**
     * Load script from HDFS by path and process it like it would be processed in Prediction Engine
     * For usage in testing or from console
     * @param scriptName
     * @param scriptParams
     * @return Integer
     * @throws Exception
     */
    public static int loadAndProcessScriptHdfs(String scriptName, String path, Object scriptParams) throws Exception {
        GroovyPredictor predictor = new GroovyPredictor(scriptName);
        return predictor.process(loadHdfsFile(path), scriptParams);
    }

    /**
     * Load script code as String from HDFS
     * @param path
     * @return String
     */
    public static String loadHdfsFile(String path) {
        Path filepath = new Path(path);
        try(FileSystem fs = FileSystem.get(filepath.toUri(), new Configuration()); BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filepath)))) {
            return reader.lines().collect(Collectors.joining("\n"));
        } catch (IOException e) {
            System.out.println("Error loading script code from " + path + " " + e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Load script code as String from local path
     * @param path
     * @return String
     */
    public static String loadLocalFile(String path) {
        try {
            return new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(path)));
        } catch(IOException e) {
            System.out.println("Error loading script code from " + path + " " + e);
            throw new RuntimeException(e);
        }
    }

    public static void main (String[] args) {
        try {
            if(args.length > 2) {
                System.out.println("RESULT = " + loadAndProcessScriptHdfs(args[0], args[1], new Object() {}));
            } else {
                System.out.println("RESULT = " + loadAndProcessScript(args[0], args[1], new Object() {}));
            }
        } catch(Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
}
