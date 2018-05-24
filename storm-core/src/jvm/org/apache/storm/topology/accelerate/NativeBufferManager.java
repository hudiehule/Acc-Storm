package org.apache.storm.topology.accelerate;

import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Administrator on 2018/3/16.
 */
public class NativeBufferManager {
    private static final Logger LOG = LoggerFactory.getLogger(NativeBufferManager.class);
    private static boolean nativeLibraryLoaded = false;
    private int[] inputShmid; // 存放已经建立过的输入共享内存标识符
    private int[] outputShmid; // 存放已经建立的输出共享内存标识符
    static {
        loadNativeLibrary();
    }

    private static void loadNativeLibrary(){
        if(!nativeLibraryLoaded){
            System.loadLibrary("StormNative");
            nativeLibraryLoaded = true;
        }
    }

    public void crateSharedMemory(int size,int[] inputShmKeys,String[] inputTupleEleTypes, int[] outputShmKeys,String[] outputTupleEleTypes){
        int inputShmNum = inputShmKeys.length;
        int outputShmNum = outputShmKeys.length;
        for(int i = 0; i<inputShmNum;i++){
            inputShmid[i] = shmGet(size,inputShmKeys[i],inputTupleEleTypes[i]);
            /*switch(inputTupleEleTypes[i]){
                case "int":{
                    inputShmid[i] = shmGet(size,inputShmKeys[i],"int");
                }
                case "boolean":{
                    inputShmid[i] = shmGet(size,inputShmKeys[i],"int");
                }
                case "byte": {
                    inputShmid[i] = shmGet(size,inputShmKeys[i],"int");
                }
                case "short": {
                    inputShmid[i] = shmGet(size,inputShmKeys[i],"int");
                }
                case "float": {
                    inputShmid[i] = shmGet(size,inputShmKeys[i],"int");
                }
                case "double": {
                    inputShmid[i] = shmGet(size,inputShmKeys[i],"int");
                }
                case "long": {
                    inputShmid[i] = shmGet(size,inputShmKeys[i],"int");
                }
                case "char": {
                    inputShmid[i] = shmGet(size,inputShmKeys[i],"int");
                }
            }*/
        }
        for(int i = 0;i<outputShmNum;i++){
            outputShmid[i] = shmGet(size,outputShmKeys[i],outputTupleEleTypes[i]);
        }
      /*  inputShmid = shmGet(size,inputShmKeys,inputTupleEleTypes);
        outputShmid = shmGet(size,outputShmKeys,outputTupleEleTypes);*/
    }

    public void clearShareMemory(){
        shmClear(inputShmid);
        shmClear(outputShmid);
    }

    public void pushInputTuplesFromBufferToShm(int size, TupleBuffers buffers){
        for(int i = 0; i < buffers.types.length;i++){
            switch(buffers.types[i]){
                case "int": {
                    int[] temp = new int[size];
                    for(int j = 0;j< size;j++){
                        temp[i] = (int)(buffers.buffers[i].get(j));
                    }
                    putIntToNativeShm(inputShmid[i],temp,size);
                    break;
                }
                case "boolean": {
                    boolean[] temp = new boolean[size];
                    for(int j = 0;j< size;j++){
                        temp[i] = (boolean)(buffers.buffers[i].get(j));
                    }
                    putBooleanToNativeShm(inputShmid[i],temp,size);
                    break;
                }
                case "short": {
                    short[] temp = new short[size];
                    for(int j = 0;j< size;j++){
                        temp[i] = (short)(buffers.buffers[i].get(j));
                    }
                    putShortToNativeShm(inputShmid[i],temp,size);
                    break;
                }
                case "byte": {
                    byte[] temp = new byte[size];
                    for(int j = 0;j< size;j++){
                        temp[i] = (byte)(buffers.buffers[i].get(j));
                    }
                    putByteToNativeShm(inputShmid[i],temp,size);
                    break;
                }
                case "float": {
                    float[] temp = new float[size];
                    for(int j = 0;j< size;j++){
                        temp[i] = (float)(buffers.buffers[i].get(j));
                    }
                    putFloatToNativeShm(inputShmid[i],temp,size);
                    break;
                }
                case "double": {
                    double[] temp = new double[size];
                    for(int j = 0;j< size;j++){
                        temp[i] = (double)(buffers.buffers[i].get(j));
                    }
                    putDoubleToNativeShm(inputShmid[i],temp,size);
                    break;
                }
                case "long": {
                    long[] temp = new long[size];
                    for(int j = 0;j< size;j++){
                        temp[i] = (long)(buffers.buffers[i].get(j));
                    }
                    putLongToNativeShm(inputShmid[i],temp,size);
                    break;
                }
                case "char": {
                    char[] temp = new char[size];
                    for(int j = 0;j< size;j++){
                        temp[i] = (char)(buffers.buffers[i].get(j));
                    }
                    putCharToNativeShm(inputShmid[i],temp,size);
                    break;
                }
            }
        }
    }

    public void pollOutputTupleEleFromShm(int size,TupleBuffers buffers){
        for(int i = 0; i <buffers.types.length;i++){
            switch(buffers.types[i]){
                case "int":{
                    int[] temp = new int[size];
                    getIntFromNativeShm(outputShmid[i],temp,size);
                    for(int j = 0;j<size;j++){
                        buffers.buffers[i].put(temp[j]);
                    }
                    break;
                }
                case "boolean":{
                    boolean[] temp = new boolean[size];
                    getBooleanFromNativeShm(outputShmid[i],temp,size);
                    for(int j = 0;j<size;j++){
                        buffers.buffers[i].put(temp[j]);
                    }
                    break;
                }
                case "short":{
                    short[] temp = new short[size];
                    getShortFromNativeShm(outputShmid[i],temp,size);
                    for(int j = 0;j<size;j++){
                        buffers.buffers[i].put(temp[j]);
                    }
                    break;
                }
                case "byte":{
                    byte[] temp = new byte[size];
                    getByteFromNativeShm(outputShmid[i],temp,size);
                    for(int j = 0;j<size;j++){
                        buffers.buffers[i].put(temp[j]);
                    }
                    break;
                }
                case "float":{
                    float[] temp = new float[size];
                    getFloatFromNativeShm(outputShmid[i],temp,size);
                    for(int j = 0;j<size;j++){
                        buffers.buffers[i].put(temp[j]);
                    }
                    break;
                }
                case "double":{
                    double[] temp = new double[size];
                    getDoubleFromNativeShm(outputShmid[i],temp,size);
                    for(int j = 0;j<size;j++){
                        buffers.buffers[i].put(temp[j]);
                    }
                    break;
                }
                case "long":{
                    long[] temp = new long[size];
                    getLongFromNativeShm(outputShmid[i],temp,size);
                    for(int j = 0;j<size;j++){
                        buffers.buffers[i].put(temp[j]);
                    }
                    break;
                }
                case "char":{
                    char[] temp = new char[size];
                    getCharFormNativeShm(outputShmid[i],temp,size);
                    for(int j = 0;j<size;j++){
                        buffers.buffers[i].put(temp[j]);
                    }
                    break;
                }
            }
        }
    }
    // 该函数返回的是创建的共享内存标识符
    public native int shmGet(int size,int shmKeys,String shmTypes);
    public native void shmClear(int[] shmids);

    public native boolean putIntToNativeShm(int shmid, int[] data,int size);
    public native boolean putLongToNativeShm(int shmid, long[] data,int size);
    public native boolean putShortToNativeShm(int shmid, short[] data, int size);
    public native boolean putByteToNativeShm(int shmid, byte[] data, int size);
    public native boolean putCharToNativeShm(int shmid, char[] data, int size);
    public native boolean putBooleanToNativeShm(int shmid, boolean[] data, int size);
    public native boolean putFloatToNativeShm(int shmid, float[] data, int size);
    public native boolean putDoubleToNativeShm(int shmid, double[] data, int size);

    public native void getIntFromNativeShm(int shmid,int[] data,int size);
    public native void getLongFromNativeShm(int shmid,long[] data, int size);
    public native void getShortFromNativeShm(int shmid,short[] data,int size);
    public native void getByteFromNativeShm(int shmid,byte[] data,int size);
    public native void getCharFormNativeShm(int shmid,char[] data,int size);
    public native void getBooleanFromNativeShm(int shmid,boolean[] data,int size);
    public native void getFloatFromNativeShm(int shmid,float[] data, int size);
    public native void getDoubleFromNativeShm(int shmid,double[] data, int size);

}
