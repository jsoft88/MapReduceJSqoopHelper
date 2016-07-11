/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.mrsqoophelper.mapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.jc.mrsqoophelper.main.CustomPropertyNamingStrategy;
import org.jc.mrsqoophelper.main.SqoopHelperMain;
import org.jc.mrsqoophelper.main.Utils;
import org.jc.mrsqoophelper.record.BaseRecord;

/**
 *
 * @author cespedjo
 */
public class Mapper extends org.apache.hadoop.mapreduce.Mapper<AvroKey<GenericData.Record>, NullWritable, Text, Text>{

    private String[] fieldConditionOutputTriple;
    private String tripletDelimiter;
    private String fqcnRecord;
    private Object instance;
    private Class clazz;
    private Schema avsc;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context); //To change body of generated methods, choose Tools | Templates.
        Configuration jobConf = context.getConfiguration();
        this.fieldConditionOutputTriple = jobConf.getStrings(SqoopHelperMain.FIELD_COND_OUTPUT_TRIPLET);
        this.tripletDelimiter = jobConf.get(SqoopHelperMain.TRIPLET_ELEMENTS_DELIMITER, ",");
        this.fqcnRecord = jobConf.getStrings(SqoopHelperMain.FQCN_RECORD_CLASS)[0];
        this.avsc = new Schema.Parser().parse(jobConf.get(SqoopHelperMain.AVRO_SCHEMA_AS_JSON));
        boolean classIsAvailable = true;
        if (!Utils.classExists(this.fqcnRecord)) {
            try {
                Utils.ClassBuilder(
                        jobConf.get(SqoopHelperMain.AVRO_SCHEMA_AS_JSON), 
                        jobConf.get(SqoopHelperMain.PACKAGE_NAME), 
                        jobConf.get(SqoopHelperMain.CLASS_ABSOLUTE_PATH), 
                        jobConf.get(SqoopHelperMain.SRC_ABSOLUTE_PATH));
                
            } catch (Exception e) {
                Logger.getLogger(Mapper.class.getName()).log(Level.SEVERE, null, e);
                classIsAvailable = false;
            }
        }
        
        if (classIsAvailable) {
            try {
                this.clazz = Class.forName(this.fqcnRecord);
                Constructor<BaseRecord> ctr = this.clazz.getDeclaredConstructor(Schema.class, String.class);
                this.instance = ctr.newInstance(avsc, jobConf.get(SqoopHelperMain.AVRO_SCHEMA_AS_JSON));
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(Mapper.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InstantiationException ex) {
                Logger.getLogger(Mapper.class.getName()).log(Level.SEVERE, null, ex);
            } catch (NoSuchMethodException ex) {
                Logger.getLogger(Mapper.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IllegalAccessException ex) {
                Logger.getLogger(Mapper.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InvocationTargetException ex) {
                Logger.getLogger(Mapper.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    @Override
    protected void map(AvroKey<GenericData.Record> key, NullWritable value, Context context) throws IOException, InterruptedException {
        Logger.getLogger(Mapper.class.getName()).log(Level.INFO, "Record:" + key.datum().toString());
        try {
            Field[] allFields = clazz.getDeclaredFields();
            for (Field f : allFields) {
                Class argClass = f.getType();
                String fieldsName = f.getName();
                //Method[] methods = clazz.getDeclaredMethods();
                Method setter = this.clazz.getDeclaredMethod("set" + fieldsName, argClass);
                f.setAccessible(true);
                Object val = key.datum().get(f.getName());
                if (f.getType().isAssignableFrom(long.class) || f.getType().isAssignableFrom(Long.class)) {
                    if (val == null) {
                        val = 0L;
                    }
                } else if (f.getType().isAssignableFrom(int.class) || f.getType().isAssignableFrom(Integer.class)) {
                    if (val == null) {
                        val = 0;
                    }
                } else if (f.getType().isAssignableFrom(short.class) || f.getType().isAssignableFrom(Short.class)) {
                    if (val == null) {
                        val = 0;
                    }
                } else if (f.getType().isAssignableFrom(Float.class) || f.getType().isAssignableFrom(float.class)) {
                    if (val == null) {
                        val = 0.0f;
                    }
                } else if (f.getType().isAssignableFrom(Double.class) || f.getType().isAssignableFrom(double.class)) {
                    if (val == null) {
                        val = 0.0;
                    }
                }
                //f.set(this.instance, val);
                setter.invoke(this.instance, val);
                Method getter = clazz.getDeclaredMethod("get" + f.getName());
                Object obj = getter.invoke(this.instance);
                System.out.println(obj);
            }
            
            ExpressionParser expParser = 
                    new ExpressionParser(this.fieldConditionOutputTriple, this.tripletDelimiter, (BaseRecord)this.instance, this.clazz);
            String outputRecordTo = expParser.evalExpressions();
            
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            
            JsonGenerator jsonGenerator = new JsonFactory().createJsonGenerator(baos, JsonEncoding.UTF8);
            jsonGenerator.useDefaultPrettyPrinter();
            JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(this.avsc, jsonGenerator);
            DatumWriter<GenericData.Record> datumWriter = new GenericDatumWriter<>(this.avsc);
            datumWriter.write(key.datum(), jsonEncoder);
            jsonEncoder.flush();
            baos.flush();
            context.write(new Text(outputRecordTo), new Text(new String(baos.toByteArray())));
        } catch (Exception ex) {
            Logger.getLogger(Mapper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
