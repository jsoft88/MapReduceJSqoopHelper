/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.mrsqoophelper.mapper;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import org.jc.mrsqoophelper.record.BaseRecord;

/**
 *
 * @author cespedjo
 */
public class ExpressionParser<T extends BaseRecord> {
    
    public static final String GREATER_THAN = "gt";
    public static final String GREATER_THAN_EQUALS = "ge";
    public static final String LESS_THAN = "lt";
    public static final String LESS_THAN_EQUALS = "le";
    public static final String EQUALS = "eq";
    public static final String NOT_EQUALS = "ne";
    
    public String field;
    
    public String operator;
    
    public String outputFile;
    
    public String castTo;
    
    public String value;
    
    public Field actualField;
    
    private T recordInstance;
    
    private List<ExpressionParser> tuples;
    
    private Stack<ExpressionParser> operationStack;
    
    public List<ExpressionParser> getTuples() {
        return this.tuples;
    }
    
    private boolean resolveDataTypeLessThan(Field f, String value, String castTo, Object fieldValue) 
            throws IllegalArgumentException, IllegalAccessException, ParseException{
        
        if (castTo.equals("java.lang.Double")) {
            return Double.class.cast(fieldValue) < Double.parseDouble(value);
        }
        
        if (castTo.equals("java.util.Date") || castTo.equals("java.sql.Date")) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            return sdf
                    .parse(String.class.cast(fieldValue)).before(sdf.parse(value));
        }
        if (f.getType().isAssignableFrom(String.class)) {
            return String.class.cast(fieldValue).compareToIgnoreCase(value) < 0;
        }
        
        if (f.getType().isAssignableFrom(Long.class) || f.getType().isAssignableFrom(long.class)) {
            return (long)fieldValue < Long.parseLong(value);
        }
        
        if (f.getType().isAssignableFrom(Integer.class) || f.getType().isAssignableFrom(int.class)) {
            return (int)fieldValue < Integer.parseInt(value);
        }
        
        if (f.getType().isAssignableFrom(Short.class) || f.getType().isAssignableFrom(short.class)) {
            return (short)fieldValue < Short.parseShort(value);
        }
        
        return false;
    }
    
    private boolean resolveDataTypeLessOrEquals(Field f, String value, String castTo, Object fieldValue) 
            throws IllegalArgumentException, IllegalAccessException, ParseException{
        
        if (castTo.equals("java.lang.Double")) {
            return Double.class.cast(fieldValue) <= Double.parseDouble(value);
        }
        
        if (castTo.equals("java.util.Date") || castTo.equals("java.sql.Date")) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            return (sdf
                    .parse(String.class.cast(fieldValue)).before(sdf.parse(value)) ||
                    sdf
                    .parse(String.class.cast(fieldValue)).equals(sdf.parse(value)));
        }
        if (f.getType().isAssignableFrom(String.class)) {
            return String.class.cast(fieldValue).compareToIgnoreCase(value) < 0
                    || String.class.cast(fieldValue).compareToIgnoreCase(value) == 0;
        }
        
        if (f.getType().isAssignableFrom(Long.class) || f.getType().isAssignableFrom(long.class)) {
            return (Long)fieldValue <= Long.parseLong(value);
        }
        
        if (f.getType().isAssignableFrom(Integer.class) || f.getType().isAssignableFrom(int.class)) {
            return (int)fieldValue <= Integer.parseInt(value);
        }
        
        if (f.getType().isAssignableFrom(Short.class) || f.getType().isAssignableFrom(short.class)) {
            return (short)fieldValue <= Short.parseShort(value);
        }
        
        return false;
    }
    
    private boolean resolveDataTypeGreaterThan(Field f, String value, String castTo, Object fieldValue) 
            throws IllegalArgumentException, IllegalAccessException, ParseException{
        return !this.resolveDataTypeLessOrEquals(f, value, castTo, fieldValue);
    }
    
    private boolean resolveDataTypeGreaterEquals(Field f, String value, String castTo, Object fieldValue) 
            throws IllegalArgumentException, IllegalAccessException, ParseException{
        return !this.resolveDataTypeLessThan(f, value, castTo, fieldValue);
    }
    
    private boolean resolveDataTypeEquals(Field f, String value, String castTo, Object fieldValue) 
            throws IllegalArgumentException, IllegalAccessException, ParseException{
        return  this.resolveDataTypeLessOrEquals(f, value, castTo, fieldValue) && 
                this.resolveDataTypeGreaterEquals(f, value, castTo, fieldValue);
    }
    
    private boolean resolveDataTypeNotEquals(Field f, String value, String castTo, Object fieldValue) 
            throws IllegalArgumentException, IllegalAccessException, ParseException{
        return !this.resolveDataTypeEquals(f, value, castTo, fieldValue);
    }
    
    private boolean applyOperator() 
            throws IllegalArgumentException, IllegalAccessException, 
            ParseException, NoSuchMethodException, SecurityException, InvocationTargetException {
        boolean retVal = true;
        while (!this.operationStack.empty()) {
            ExpressionParser exp1 = this.operationStack.pop();
            exp1.actualField.setAccessible(true);
            Object val = 
                    this.recordInstance
                            .getClass()
                            .getDeclaredMethod("get" + exp1.actualField.getName())
                            .invoke(this.recordInstance);
            
            switch (exp1.operator) {
                case GREATER_THAN:
                    retVal = retVal && 
                            this.resolveDataTypeGreaterThan(exp1.actualField, exp1.value, exp1.castTo, val);
                    break;
                case GREATER_THAN_EQUALS:
                    retVal = retVal &&
                            this.resolveDataTypeGreaterEquals(exp1.actualField, exp1.value, exp1.castTo, val);
                    break;
                case LESS_THAN:
                    retVal = retVal &&
                            this.resolveDataTypeLessThan(exp1.actualField, exp1.value, exp1.castTo, val);
                    break;
                case LESS_THAN_EQUALS:
                    retVal = retVal &&
                            this.resolveDataTypeLessOrEquals(exp1.actualField, exp1.value, exp1.castTo, val);
                    break;
                case EQUALS:
                    retVal = retVal &&
                            this.resolveDataTypeEquals(exp1.actualField, exp1.value, exp1.castTo, val);
                    break;
                case NOT_EQUALS:
                    retVal = retVal &&
                            this.resolveDataTypeNotEquals(exp1.actualField, exp1.value, exp1.castTo, val);
                    break;
                default:
                    retVal = false;
            }
        }
        
        return retVal;
    }
    
    public String evalExpressions() 
            throws IllegalArgumentException, IllegalAccessException, ParseException, 
            NoSuchMethodException, SecurityException, InvocationTargetException {
        //O(n^2 - n) but it doesn't matter because there aren't many files.
        int slotForInsert;
        for (int i = 0; i < this.tuples.size() - 1; i = slotForInsert + 1) {
            slotForInsert = i;
            for (int j = i + 1; j < this.tuples.size(); ++j) {
                if (this.tuples.get(i).value.compareToIgnoreCase(this.tuples.get(j).value) == 0) {
                    if (slotForInsert + 1 < j) {
                        ExpressionParser val = this.tuples.get(++slotForInsert);
                        this.tuples.set(slotForInsert, this.tuples.get(j));
                        this.tuples.set(j, val);
                    } else {
                        ++slotForInsert;
                    }
                }
            }
            if (slotForInsert == i) {
                slotForInsert = 0;
            }
        }
        String file = this.tuples.get(0).outputFile;
        for (int i = 0; i < this.tuples.size(); ++i) {
            if (this.tuples.get(i).outputFile.equals(file)) {
                this.operationStack.push(this.tuples.get(i));
            } else {
                if (applyOperator()) {
                    return file;
                }
                file = this.tuples.get(i).outputFile;
                this.operationStack.push(this.tuples.get(i));
            }
        }
        if (applyOperator()) {
            return file;
        }
        
        return null;
    }
    
    private ExpressionParser(String field, String operator, String value, String outputFile, String castTo, Field actualField) {
        this.field = field;
        this.operator = operator;
        this.outputFile = outputFile;
        this.castTo = castTo;
        this.value = value;
        this.actualField = actualField;
    } 
    
    public ExpressionParser(String[] tuples, String tupleDelim, T record, Class recordClass) 
            throws Exception {
        this.operationStack = new Stack<>();
        this.tuples = new ArrayList<>(tuples.length);
        for (String aTriplet : tuples) {
            String[] elements = aTriplet.split(tupleDelim);
            if (elements.length < 5) {
                throw new Exception("Incorrect tuple.");
            }
            ExpressionParser exp = 
                    new ExpressionParser(
                            elements[0], 
                            elements[1], 
                            elements[2], 
                            elements[3], 
                            elements[4], 
                            recordClass.getDeclaredField(elements[0]));
            this.tuples.add(exp);
            this.recordInstance = record;
        }
    }
}
