/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.mrsqoophelper.main;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.avro.Schema;
import org.jc.avsctoclass.ClassFromAvro;
import org.jc.avsctoclass.ClassFromAvroFactory;
import org.jc.mrsqoophelper.record.BaseRecord;

/**
 *
 * @author cespedjo
 */
public class Utils {
    
    public static boolean classExists(String fqcn) {
        try {
           Class clazz = Class.forName(fqcn);
           return clazz != null;
        } catch (ClassNotFoundException ex) {
            return false;
        }
    }
    
    public static Class ClassBuilder(String schemaAsJson, 
            String packageName, String absolutePathClasspath, String absoluteSrcPath) 
            throws Exception {
        ClassFromAvro cfa = new ClassFromAvroFactory().getInstance();
        Schema avsc = new Schema.Parser().parse(schemaAsJson);
        String sd = packageName.replace(".", File.separator);
        
        Path pSrc = Paths.get(absoluteSrcPath);
        Path pCls = Paths.get(absolutePathClasspath);
        
        Path classPathPath = pCls.resolve(Paths.get(sd));
        Path srcAbsolutePath = pSrc.resolve(Paths.get(sd));

        cfa.init(avsc, classPathPath, srcAbsolutePath, packageName);
        
        return cfa.generateClass(
                BaseRecord.class.getName(), 
                new String[]{Schema.class.getName(), String.class.getName()}, 
                "{super($1,$2);}",
                "public " + avsc.getName() + "(Schema schema, String recordAsJson) {super(schema, recordAsJson);}");
    }
}
