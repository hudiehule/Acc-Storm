package org.apache.storm.topology.accelerate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Administrator on 2018/11/7.
 */
class TupleElementBuffer{
    private static final Logger LOG = LoggerFactory.getLogger(TupleElementBuffer.class);
    byte[] bytes = null;
    short[] shorts = null;
    int[] ints = null;
    long[] longs = null;

    float[] floats = null;
    double[] doubles = null;
    boolean[] booleans = null;

    char[] chars = null;
    String[] strings = null;
    DataType dataType;
    public boolean isArray;
    public int batchSize;
    public int arraySize;
    private int N;
    public int bufferSize;
    public TupleElementBuffer(DataType type, boolean isArray, int batchSize, int arraySize){
        this.dataType = type;
        this.isArray = isArray;
        this.batchSize = batchSize;
        this.arraySize =arraySize;
        this.N = 0;
        this.bufferSize = batchSize * arraySize;
        LOG.info("create a buffer , the size = " + bufferSize);
        switch(type){
            case BYTE: {
                bytes = new byte[bufferSize];
                break;
            }
            case SHORT: {
                shorts = new short[bufferSize];
                break;
            }
            case INT: {
                ints = new int[bufferSize];
                break;
            }
            case LONG: {
                longs = new long[bufferSize];
                break;
            }
            case FLOAT: {
                floats = new float[bufferSize];
                break;
            }
            case DOUBLE: {
                doubles = new double[bufferSize];
                break;
            }
            case BOOLEAN: {
                booleans = new boolean[bufferSize];
                break;
            }
            case CHAR: {
                chars = new char[bufferSize];
                break;
            }
            case STRING: {
                strings = new String[bufferSize];
                break;
            }
        }
    }

    int getLength(){
        return N;
    }
    void resetN(){
        this.N = 0;
    }
    void put(Object value){
        switch(dataType){
            case INT: {
                putInt(value);
                break;
            }
            case FLOAT: {
                putFloat(value);
                break;
            }
            case BYTE: {
                putByte(value);
                break;
            }
            case SHORT: {
                putShort(value);
                break;
            }
            case LONG: {
                putLong(value);
                break;
            }
            case DOUBLE: {
                putDouble(value);
                break;
            }
            case BOOLEAN: {
                putBoolean(value);
                break;
            }
            case CHAR: {
                putChar(value);
                break;
            }
            case STRING: {
                putString(value);
                break;
            }
        }
    }
    private void putByte(Object value){
        if(isArray){
            byte[] data = (byte[])value;
            for(int i = 0; i< arraySize;i++){
                bytes[N++] = data[i];
            }
        }else{
            bytes[N++] = (byte)value;
        }
    }
    private void putShort(Object value){
        if(isArray){
            short[] data = (short[])value;
            for(int i = 0; i< arraySize;i++){
                shorts[N++] = data[i];
            }
        }else{
            shorts[N++] = (short)value;
        }
    }
    private void putInt(Object value){
        if(isArray){
            int[] data = (int[])value;
            for(int i = 0; i< arraySize;i++){
                ints[N++] = data[i];
            }
        }else{
            ints[N++] = (int)value;
        }
    }
    private void putLong(Object value){
        if(isArray){
            long[] data = (long[])value;
            for(int i = 0; i< arraySize;i++){
                longs[N++] = data[i];
            }
        }else{
            longs[N++] = (long)value;
        }
    }
    private void putFloat(Object value){
        if(isArray){
            float[] data = (float[])value;
            for(int i = 0; i< arraySize;i++){
                floats[N++] = data[i];
            }
        }else{
            floats[N++] = (float)value;
        }
    }
    private void putDouble(Object value){
        if(isArray){
            double[] data = (double[])value;
            for(int i = 0; i< arraySize;i++){
                doubles[N++] = data[i];
            }
        }else{
            doubles[N++] = (double)value;
        }
    }
    private void putBoolean(Object value){
        if(isArray){
            boolean[] data = (boolean[])value;
            for(int i = 0; i< arraySize;i++){
                booleans[N++] = data[i];
            }
        }else{
            booleans[N++] = (boolean)value;
        }
    }
    private void putChar(Object value){
        if(isArray){
            char[] data = (char[])value;
            for(int i = 0; i< arraySize;i++){
                chars[N++] = data[i];
            }
        }else{
            chars[N++] = (char)value;
        }
    }

    private void putString(Object value){
        if(isArray){
            String[] data = (String[])value;
            for(int i = 0; i< arraySize;i++){
                strings[N++] = data[i];
            }
        }else{
            strings[N++] = (String)value;
        }
    }

    Object get(int index){
        switch (dataType){
            case BYTE: {
                return bytes[index];
            }
            case SHORT: {
                return shorts[index];
            }
            case INT: {
                return ints[index];
            }
            case LONG: {
                return longs[index];
            }
            case FLOAT: {
                return floats[index];
            }
            case DOUBLE: {
                return doubles[index];
            }
            case BOOLEAN: {
                return booleans[index];
            }
            case CHAR: {
                return chars[index];
            }
            case STRING: {
                return strings[index];
            }
        }
        return null;
    }
    Object[] getArray(int index){
        Object[] data = new Object[arraySize];
        int start = index * arraySize;
        int end = start + arraySize;
        LOG.info("startIndex = " + start + " ,endIndex = " + end);
        switch (dataType){
            case BYTE: {
                for(int i = start; i < end; i++){
                    data[i] = bytes[i];
                }
                return data;
            }
            case SHORT: {
                for(int i = start; i < end; i++){
                    data[i] = shorts[i];
                }
                return data;
            }
            case INT: {
                for(int i = start; i < end; i++){
                    data[i] = ints[i];
                }
                return data;
            }
            case LONG: {
                for(int i = start; i < end; i++){
                    data[i] = longs[i];
                }
                return data;
            }
            case FLOAT: {
                LOG.info("float buffer size = " + floats.length);
                for(int i = start; i < end; i++){
                    data[i] = floats[i];
                }
                return data;
            }
            case DOUBLE: {
                for(int i = start; i < end; i++){
                    data[i] = doubles[i];
                }
                return data;
            }
            case BOOLEAN: {
                for(int i = start; i < end; i++){
                    data[i] = booleans[i];
                }
                return data;
            }
            case CHAR: {
                for(int i = start; i < end; i++){
                    data[i] = chars[i];
                }
                return data;
            }
            case STRING: {
                for(int i = start; i < end; i++){
                    data[i] = strings[i];
                }
                return data;
            }
        }
        return data;
    }

    public byte[] getByteBuffer() {
        return bytes;
    }

    public short[] getShortBuffer() {
        return shorts;
    }

    public int[] getIntBuffer() {
        return ints;
    }

    public long[] getLongBuffer() {
        return longs;
    }

    public float[] getFloatBuffer() {
        return floats;
    }

    public double[] getDoubleBuffer() {
        return doubles;
    }

    public boolean[] getBooleanBuffer() {
        return booleans;
    }

    public char[] getCharBuffer() {
        return chars;
    }

    public String[] getStringBuffer() {
        return strings;
    }
}
