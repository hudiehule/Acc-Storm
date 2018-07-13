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
    private static final String INPUT_AND_OUTPUT_FLAG_TYPE = "INPUT_AND_OUTPUT_FLAG_TYPE";
    private static int shmFlagid;
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

    public void crateSharedMemory(int size,String[] inputTupleEleTypes,String[] outputTupleEleTypes){
        int inputShmNum = inputTupleEleTypes.length;
        int outputShmNum = outputTupleEleTypes.length;
        inputShmid = new int[inputShmNum];
        outputShmid = new int[outputShmNum];
        for(int i = 0; i<inputShmNum;i++){
            inputShmid[i] = shmGet(size,inputTupleEleTypes[i]);
        }
        for(int i = 0;i<outputShmNum;i++){
            outputShmid[i] = shmGet(size,outputTupleEleTypes[i]);
        }
      /*  inputShmid = shmGet(size,inputShmKeys,inputTupleEleTypes);
        outputShmid = shmGet(size,outputShmKeys,outputTupleEleTypes);*/
    }

    public void createInputAndOutputFlagShm(){
        shmFlagid = shmGet(1,INPUT_AND_OUTPUT_FLAG_TYPE);
    }

  /*  public void clearShareMemory(){
        int[] shmids = new int[inputShmid.length+outputShmid.length+1];
        int index = 0;
        for(int i : inputShmid){
            shmids[index++] = i;
        }
        for(int i : outputShmid){
            shmids[index++] = i;
        }
        shmids[index] = shmFlagid;
        shmClear(shmids);
    }*/

    public void setKernelInputFlagEnd(){
        setInputDataEnd(shmFlagid);
    }

    public int[] getInputShmids(){
        return inputShmid;
    }

    public int[] getOutputShmids(){
        return outputShmid;
    }

    public int getShmFlagid(){
        return shmFlagid;
    }
    public void pushInputTuplesFromBufferToShmAndStartKernel(int size, TupleBuffers buffers){
        //等待input data flag的值为0 表示可以向共享内存传送数据了
        waitInputDataConsumed(shmFlagid);
        for(int i = 0; i < buffers.types.length;i++){
            switch(buffers.types[i]){
                case "int": {
                    int[] temp = new int[size];
                    for(int j = 0;j< size;j++){
                        temp[j] = (int)(buffers.buffers[i].get(j));
                    }
                    putIntToNativeShm(inputShmid[i],temp,size);
                    break;
                }
                case "boolean": {
                    boolean[] temp = new boolean[size];
                    for(int j = 0;j< size;j++){
                        temp[j] = (boolean)(buffers.buffers[i].get(j));
                    }
                    putBooleanToNativeShm(inputShmid[i],temp,size);
                    break;
                }
                case "short": {
                    short[] temp = new short[size];
                    for(int j = 0;j< size;j++){
                        temp[j] = (short)(buffers.buffers[i].get(j));
                    }
                    putShortToNativeShm(inputShmid[i],temp,size);
                    break;
                }
                case "byte": {
                    byte[] temp = new byte[size];
                    for(int j = 0;j< size;j++){
                        temp[j] = (byte)(buffers.buffers[i].get(j));
                    }
                    putByteToNativeShm(inputShmid[i],temp,size);
                    break;
                }
                case "float": {
                    float[] temp = new float[size];
                    for(int j = 0;j< size;j++){
                        temp[j] = (float)(buffers.buffers[i].get(j));
                    }
                    putFloatToNativeShm(inputShmid[i],temp,size);
                    break;
                }
                case "double": {
                    double[] temp = new double[size];
                    for(int j = 0;j< size;j++){
                        temp[j] = (double)(buffers.buffers[i].get(j));
                    }
                    putDoubleToNativeShm(inputShmid[i],temp,size);
                    break;
                }
                case "long": {
                    long[] temp = new long[size];
                    for(int j = 0;j< size;j++){
                        temp[j] = (long)(buffers.buffers[i].get(j));
                    }
                    putLongToNativeShm(inputShmid[i],temp,size);
                    break;
                }
                case "char": {
                    char[] temp = new char[size];
                    for(int j = 0;j< size;j++){
                        temp[j] = (char)(buffers.buffers[i].get(j));
                    }
                    putCharToNativeShm(inputShmid[i],temp,size);
                    break;
                }
            }
        }
        //设置input flag 为1 表示共享内存的数据已经准备好 内核可以进行计算了
        setInputDataReady(shmFlagid);
    }

    public void waitAndPollOutputTupleEleFromShm(int size,TupleBuffers buffers){
        //首先等待outputFlag 的值为1 表示结果可取
        waitOutputDataReady(shmFlagid);
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
        setOutputDataConsumed(shmFlagid);
    }
    // 该函数返回的是创建的共享内存标识符
    public native int shmGet(int size,String shmTypes);
    // public native void shmClear(int[] shmids);

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

    public native void setInputDataReady(int shmid); //将inputFlag的值设置为1 表示输入的数据可用了
    public native void waitOutputDataReady(int shmid);  //阻塞等待ouputFlag的值为1 为1 就返回 不为1 就循环等待
    public native void setOutputDataConsumed(int shmid); // 将outputFlag 设置为0
    public native void setInputDataEnd(int shmid); //设置inputFlag 的值为-1 表示这个bolt要结束了 对应的kernel函数以及相关资源都可以进行清理了
    public native void waitInputDataConsumed(int shmid); //等待共享内存中input 数据被消费 这样就可以传送第二批数据到共享内存了
}
