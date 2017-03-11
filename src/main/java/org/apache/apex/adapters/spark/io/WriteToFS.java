package org.apache.apex.adapters.spark.io;

import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import org.apache.apex.adapters.spark.operators.CountByValueOperator;
import org.apache.apex.adapters.spark.properties.PathProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by anurag on 24/2/17.
 */
public class WriteToFS {
    static Logger log = LoggerFactory.getLogger(CountByValueOperator.class);
    public static void write(String path, Object o){
        PathProperties properties = new PathProperties();
        String fs =properties.getProperty("fs").toLowerCase();
        String baseHDFSPath = properties.getProperty("baseHDFSPath");
        switch (fs){
            case "alluxio":
                writeFileToAlluxio(path,o);
                break;
            case "local":
                writeFileToLocal(path, o);
                break;
            case "hdfs":
                writeFileToHDFS(baseHDFSPath+path, o);
                break;
        }
    }
    public static void deleteSUCCESSFile(){
        PathProperties properties = new PathProperties();
        String fs =properties.getProperty("fs").toLowerCase();

        switch (fs){
            case "alluxio":
                 deleteSUCCESSFileAlluxio();
                break;
            case "local":
                deleteSUCCESSFileLocal();
                break;
            case "hdfs":
                deleteSUCCESSFileHDFS();
                break;
            default:return ;
        }
    }

    public synchronized static void deleteSUCCESSFileLocal() {
    }

    public synchronized static void deleteSUCCESSFileHDFS() {
        Configuration conf = new Configuration(true);
        PathProperties properties = new PathProperties();
        String successHDFS = properties.getProperty("successHDFS");
        Path pt=new Path(successHDFS);
        try {
            org.apache.hadoop.fs.FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(pt.toUri(), conf);
            if(hdfs.exists(pt)){
                hdfs.delete(pt,false);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static synchronized void createSuccessFile() {
        FileSystem fs = FileSystem.Factory.get();
        AlluxioURI pathURI = new AlluxioURI("/user/anurag/spark-apex/_SUCCESS");
        try {
            FileOutStream outStream = fs.createFile(pathURI);
            DataOutputStream ds = new DataOutputStream(outStream);
            ds.write("Success File Created".getBytes());
            ds.flush();
            ds.close();
            outStream.flush();
            outStream.close();
        }
        catch (AlluxioException | IOException e) {
            throw new RuntimeException(e);
        }

    }
    public static synchronized  void writeFileToAlluxio(String path,Object o) {
        try {
            FileSystem fs = FileSystem.Factory.get();
            AlluxioURI pathURI = new AlluxioURI("/user/anurag/spark-apex/" + path);
            if (fs.exists(pathURI))
                fs.delete(pathURI);
            FileOutStream outStream = fs.createFile(pathURI);
            ObjectOutputStream oos = new ObjectOutputStream(outStream);
            oos.writeObject(o);
            oos.flush();
            oos.close();
            outStream.flush();
            outStream.close();
            createSuccessFile();
        }
        catch (AlluxioException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized static void deleteSUCCESSFileAlluxio() {
        try {
            alluxio.client.file.FileSystem fs = alluxio.client.file.FileSystem.Factory.get();
            AlluxioURI pathURI=new AlluxioURI("/user/anurag/spark-apex/_SUCCESS");
            if(fs.exists(pathURI)) fs.delete(pathURI);

        } catch (IOException | AlluxioException e) {
            throw new RuntimeException(e);
        }

    }
    public synchronized static void writeFileToHDFS(String path,Object o){
        Configuration configuration = new Configuration(true);
        log.info("path to write {}",path);
        try {
            Path file = new Path(path);
            org.apache.hadoop.fs.FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(file.toUri(), configuration);
            if (hdfs.exists(file)) {
                hdfs.delete(file, true);
            }
            OutputStream os = hdfs.create(file);
            ObjectOutputStream oos=new ObjectOutputStream(os);
                oos.writeObject(o);
                oos.flush();
                oos.close();
            createHDFSSuccessFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static synchronized void createHDFSSuccessFile() {
        PathProperties properties = new PathProperties();
        String successHDFS = properties.getProperty("successHDFS");
        Configuration configuration = new Configuration(true);
        try {
            Path file = new Path(successHDFS);
            org.apache.hadoop.fs.FileSystem hdfs = org.apache.hadoop.fs.FileSystem.get(file.toUri(), configuration);
            if (hdfs.exists(file)) {
                hdfs.delete(file, true);
            }
            hdfs.create(file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
    public synchronized  static void writeFileToLocal(String path,Object o) {
        try {
            FileOutputStream fos = new FileOutputStream(path);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(o);
            oos.flush();
            oos.close();

            FileOutputStream fileWriter = new FileOutputStream("/tmp/spark-apex/_SUCCESS");
            fileWriter.write("Writing Data to file".getBytes());
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
