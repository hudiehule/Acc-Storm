package org.apache.storm.topology.accelerate;

import org.apache.storm.tuple.Values;
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
            for(int i = 0; i< data.length;i++){
                chars[N++] = data[i];
            }
            for(int i = data.length; i< arraySize;i++){
                chars[N++] = '\0';
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

    public void putDataIntoValues(Values values,int index){
        int start = index * arraySize;
        int end = start + arraySize;
        switch (dataType){
            case BYTE: {
                if(isArray){
                    byte[] data = new byte[arraySize];
                    for(int i = start,j = 0; i < end; i++,j++){
                        data[j] = bytes[i];
                    }
                    values.add(data);
                }else{
                    values.add(bytes[index]);
                }
                break;
            }
            case SHORT: {
                if(isArray){
                    short[] data = new short[arraySize];
                    for(int i = start,j = 0; i < end; i++,j++){
                        data[j] = shorts[i];
                    }
                    values.add(data);
                }else{
                    values.add(shorts[index]);
                }
                break;
            }
            case INT: {
                if(isArray){
                    int[] data = new int[arraySize];
                    for(int i = start,j = 0; i < end; i++,j++){
                        data[j] = ints[i];
                    }
                    values.add(data);
                }else{
                    values.add(ints[index]);
                }
                break;
            }
            case LONG: {
                if(isArray){
                    long[] data = new long[arraySize];
                    for(int i = start,j = 0; i < end; i++,j++){
                        data[j] = longs[i];
                    }
                    values.add(data);
                }else{
                    values.add(longs[index]);
                }
                break;
            }
            case FLOAT: {
                if(isArray){
                    float[] data = new float[arraySize];
                    for(int i = start,j = 0; i < end; i++,j++){
                        data[j] = floats[i];
                    }
                    values.add(data);
                }else{
                    values.add(floats[index]);
                }
                break;
            }
            case DOUBLE: {
                if(isArray){
                    double[] data = new double[arraySize];
                    for(int i = start,j = 0; i < end; i++,j++){
                        data[j] = doubles[i];
                    }
                    values.add(data);
                }else{
                    values.add(doubles[index]);
                }
                break;
            }
            case BOOLEAN: {
                if(isArray){
                    boolean[] data = new boolean[arraySize];
                    for(int i = start,j = 0; i < end; i++,j++){
                        data[j] = booleans[i];
                    }
                    values.add(data);
                }else{
                    values.add(booleans[index]);
                }
                break;
            }
            case CHAR: {
                if(isArray){
                    char[] data = new char[arraySize];
                    for(int i = start,j = 0; i < end; i++,j++){
                        data[j] = chars[i];
                    }
                    values.add(data);
                }else{
                    values.add(chars[index]);
                }
                break;
            }
            case STRING: {
                if(isArray){
                    String[] data = new String[arraySize];
                    for(int i = start,j = 0; i < end; i++,j++){
                        data[j] = strings[i];
                    }
                    values.add(data);
                }else{
                    values.add(strings[index]);
                }
                break;
            }
        }
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
