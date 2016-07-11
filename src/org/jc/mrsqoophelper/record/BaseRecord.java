/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.mrsqoophelper.record;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.annotate.JacksonInject;

/**
 *
 * @author cespedjo
 */
public class BaseRecord extends GenericData.Record {

    @JsonIgnore private GenericDatumWriter<GenericData.Record> writer;
    @JsonIgnore private DatumReader<GenericData.Record> reader;
    @JsonIgnore private JsonEncoder enc;
    @JsonIgnore private ByteArrayOutputStream baos;
    @JsonIgnore private JsonDecoder dec;
    @JsonIgnore private Schema schema;
    
    public BaseRecord(@JacksonInject Schema schema, @JacksonInject String recordAsJson) {
        this(schema);
        try {
            this.dec = DecoderFactory.get().jsonDecoder(schema, recordAsJson);
        } catch (IOException ex) {
            Logger.getLogger(BaseRecord.class.getName()).log(Level.SEVERE, null, ex);
            this.dec = null;
        }
    }
    
    public BaseRecord() {
        this(null, null);
    }
    
    public BaseRecord(Schema schema) {
        super(schema);
        this.schema = schema;
        this.baos = new ByteArrayOutputStream();
        this.dec = null;
        try {
            this.enc = EncoderFactory.get().jsonEncoder(schema, baos);
        } catch (IOException ex) {
            Logger.getLogger(BaseRecord.class.getName()).log(Level.SEVERE, null, ex);
            this.enc = null;
        }
        this.writer = new GenericDatumWriter<>();
        this.reader = new GenericDatumReader<>();
    }
    
    //TODO: Check why this doesn't work.
    public <T extends BaseRecord> CharSequence toJson(T instance) 
            throws Exception {
        if (this.enc == null) {
            throw new Exception("Record not initialized correctly.");
        }
        
        writer.setSchema(schema);
        writer.write(instance, enc);
        enc.flush();
        return new String(baos.toByteArray());
    }
    
    public <T extends BaseRecord> BaseRecord fromJson(T instance) 
            throws Exception {
        if (this.dec == null) {
            throw new Exception("Record not initialized correctly.");
        }
        
        return (BaseRecord)this.reader.read(instance, this.dec);
    }
}
