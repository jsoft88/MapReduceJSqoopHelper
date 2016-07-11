/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.mrsqoophelper.reducer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.codehaus.jackson.map.InjectableValues;
import org.codehaus.jackson.map.ObjectMapper;
import org.jc.mrsqoophelper.main.SqoopHelperMain;
import org.jc.mrsqoophelper.main.Utils;
import org.jc.mrsqoophelper.mapper.Mapper;
import org.jc.mrsqoophelper.record.BaseRecord;

/**
 *
 * @author cespedjo
 */
public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, NullWritable, NullWritable>{
    
    private String fqcnRecordClassName;
    
    private GenericDatumWriter<Object> writer;
    
    private DataFileWriter<Object> dataFileWriter;
    
    private String avroSchemaAsJson;
    
    private Schema schema;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration jobConf = context.getConfiguration();
        this.fqcnRecordClassName = jobConf.getStrings(SqoopHelperMain.FQCN_RECORD_CLASS)[0];
        this.avroSchemaAsJson = jobConf.get(SqoopHelperMain.AVRO_SCHEMA_AS_JSON);
        
        if (!Utils.classExists(this.fqcnRecordClassName)) {
            try {
                Utils.ClassBuilder(
                        jobConf.get(this.avroSchemaAsJson), 
                        jobConf.get(SqoopHelperMain.PACKAGE_NAME), 
                        jobConf.get(SqoopHelperMain.CLASS_ABSOLUTE_PATH), 
                        jobConf.get(SqoopHelperMain.SRC_ABSOLUTE_PATH));
            } catch (Exception e) {
                Logger.getLogger(Mapper.class.getName()).log(Level.SEVERE, null, e);
            }
        }
        this.schema = new Schema.Parser().parse(this.avroSchemaAsJson);
        this.writer = new GenericDatumWriter<>(this.schema);
        this.writer.setSchema(this.schema);
        this.dataFileWriter = new DataFileWriter<>(writer);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        try {
            Class[] cArgs = new Class[2];
            cArgs[0] = Schema.class;
            cArgs[1] = String.class;
            
            Class clazz = Class.forName(this.fqcnRecordClassName);
            ObjectMapper mapper = new ObjectMapper();
            final InjectableValues.Std injectableValues = new InjectableValues.Std();
            injectableValues.addValue(cArgs[0], this.schema);
            injectableValues.addValue(cArgs[1], this.avroSchemaAsJson);
            this.dataFileWriter.appendTo(new File(key.toString()));
            for (Text aValue : values) {
                String val = aValue.toString();
                /*
                BaseRecord instance = (BaseRecord)clazz
                        .getDeclaredConstructor(cArgs)
                        .newInstance(this.schema, val);*/
                
                //Method fromJson = clazz.getMethod("fromJson", BaseRecord.class);
                //instance = (BaseRecord)fromJson.invoke(instance, instance);
                InputStream input = new ByteArrayInputStream(val.getBytes());
                DataInputStream din = new DataInputStream(input);
                Decoder decoder = DecoderFactory.get().jsonDecoder(this.schema, input);
                
                DatumReader<Object> reader = new GenericDatumReader<>(this.schema);
                this.dataFileWriter.append(reader.read(null, decoder));
            }
            this.dataFileWriter.flush();
            this.dataFileWriter.close();
        } catch (/*ClassNotFoundException | NoSuchMethodException */ Exception ex) {
            Logger.getLogger(Reducer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
   
}
