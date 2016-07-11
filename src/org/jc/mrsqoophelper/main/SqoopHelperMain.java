/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.mrsqoophelper.main;

import com.github.sakserv.minicluster.impl.YarnLocalCluster;
import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.jc.mrsqoophelper.mapper.Mapper;
import org.jc.mrsqoophelper.reducer.Reducer;

/**
 *
 * @author cespedjo
 */
public class SqoopHelperMain {
    
    //public static final String CONDITIONS_DELIMITER = " ";
    public static final String OUTPUT_FILES = "outfiles"; 
    public static final String FIELD_COND_OUTPUT_TRIPLET = "fcot";
    public static final String TRIPLET_ELEMENTS_DELIMITER = "tdel";
    public static final String FQCN_RECORD_CLASS = "fqcn";
    public static final String AVRO_SCHEMA_AS_JSON = "avsj";
    public static final String SRC_ABSOLUTE_PATH = "srcpa";
    public static final String CLASS_ABSOLUTE_PATH = "clspa";
    public static final String PACKAGE_NAME = "pkgnam";
    private static final String TRIPLET_ELEMENTS_DELIMITER_SYMBOL = "=";
    private static final String CMD_LINE_TRIPLET_ORIGINAL_DELIM = ",";
    
    public static void main(String[] args) {
        if (args == null || args.length < 6) {
            System.out.println("Incorrect usage of tool:");
            System.out.println("hadoop jar program.jar <package_name> <json_schema_file> <jar_classpath> <absolute_src_path> <field constraint output_file cast_to;field constraint output_path> <input_file_1;input_file_2;input_file_3> <output_file1;output_file2;...");
            return;
        }
        
        String packageName = args[0];
        String jsonPath = args[1];
        String classPath = args[2];
        String absolutePathOfSrc = args[3];
        //Pair field constraint ouput_file, do not join with logical operators.
        //Example: date gt 'yyyy/MM/dd' ouput_file
        //         date lt 'yyyy/MM/dd' output_file
        //This is correct!
        //But:
        //date gt 'yyyy/MM/dd' and date lt 'yyyy/MM/dd'
        //This is wrong!!
        //Conditions will be appended with AND
        //separate conditions with semi-colons.
        //conditions with same output file will be appended with AND operator
        String fieldToFilter = args[4];
        //separate input files by comma
        String inputAvroFiles = args[5];
        String outputAvroFiles = args[6];
        
        try {
            Path pathOfSchema = Paths.get(jsonPath);
            fieldToFilter =
                    fieldToFilter.replace(
                            CMD_LINE_TRIPLET_ORIGINAL_DELIM, 
                            TRIPLET_ELEMENTS_DELIMITER_SYMBOL);
            String schemaAsJson = Files.readAllLines(pathOfSchema, Charset.forName("UTF-8")).get(0);
            Class recordClass = Utils.ClassBuilder(schemaAsJson, packageName, classPath, absolutePathOfSrc);
            Configuration conf = new Configuration();
            
            conf.setStrings(FIELD_COND_OUTPUT_TRIPLET, fieldToFilter.split(";"));
            conf.set(TRIPLET_ELEMENTS_DELIMITER, TRIPLET_ELEMENTS_DELIMITER_SYMBOL);
            conf.set(FQCN_RECORD_CLASS, recordClass.getName());
            conf.set(AVRO_SCHEMA_AS_JSON, schemaAsJson);
            conf.set(SRC_ABSOLUTE_PATH, absolutePathOfSrc);
            conf.set(CLASS_ABSOLUTE_PATH, classPath);
            conf.set(PACKAGE_NAME, packageName);
            
            Job job = new Job(conf);
            //Set context for mapper and reducer.
            job.setJobName("Filter Records for Sqoop Export");
            job.setInputFormatClass(AvroKeyInputFormat.class);
            job.setMapperClass(Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(Reducer.class);
            String[] argPaths = inputAvroFiles.split(";");
            org.apache.hadoop.fs.Path[] paths = 
                    new org.apache.hadoop.fs.Path[argPaths.length];
            for (int i = 0; i < argPaths.length; ++i) {
                paths[i] = new org.apache.hadoop.fs.Path(argPaths[i]);
            }
            
            org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, paths);
            LazyOutputFormat.setOutputFormatClass(job, NullOutputFormat.class);
            
            //creating output files
            Schema avsc = new Schema.Parser().parse(schemaAsJson);
            DataFileWriter<Object> dfw = new DataFileWriter<>(new GenericDatumWriter<>(avsc));
            for (String out : outputAvroFiles.split(";")) {
                dfw.create(avsc, new File(out));
                dfw.close();
            }
            /*YarnLocalCluster yarnLocalCluster = new YarnLocalCluster.Builder()
            .setNumNodeManagers(1)
            .setNumLocalDirs(Integer.parseInt("1"))
            .setNumLogDirs(Integer.parseInt("1"))
            .setResourceManagerAddress("localhost")
            .setResourceManagerHostname("localhost:37001")
            .setResourceManagerSchedulerAddress("localhost:37002")
            .setResourceManagerResourceTrackerAddress("localhost:37003")
            .setResourceManagerWebappAddress("localhost:37004")
            .setUseInJvmContainerExecutor(true)
            .setConfig(conf)
            .build();
            


            yarnLocalCluster.start();*/
            
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception ex) {
            Logger.getLogger(SqoopHelperMain.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Could not generate class for avro schema.");
        }         
        
    }
}
