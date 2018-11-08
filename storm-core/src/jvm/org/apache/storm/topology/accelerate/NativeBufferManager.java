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
    private static final int INPUT_AND_OUTPUT_FLAG_TYPE = 9;
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

    public void crateSharedMemory(int[] inputBuffersize,DataType[] inputTupleEleTypes,int[] outputBufferSize, DataType[] outputTupleEleTypes){
        int inputShmNum = inputTupleEleTypes.length;
        int outputShmNum = outputTupleEleTypes.length;
        inputShmid = new int[inputShmNum];
        outputShmid = new int[outputShmNum];
        for(int i = 0; i<inputShmNum;i++){
            inputShmid[i] = shmGet(inputBuffersize[i],inputTupleEleTypes[i].dataTypeFlag);
        }
        for(int i = 0;i<outputShmNum;i++){
            outputShmid[i] = shmGet(outputBufferSize[i],outputTupleEleTypes[i].dataTypeFlag);
        }
      /*  inputShmid = shmGet(size,inputShmKeys,inputTupleEleTypes);
        outputShmid = shmGet(size,outputShmKeys,outputTupleEleTypes);*/
    }

    public void createInputAndOutputFlagShm(){
        shmFlagid = shmGet(1,INPUT_AND_OUTPUT_FLAG_TYPE);
    }


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
    public void pushInputTuplesFromBufferToShmAndStartKernel(int[] sizes, TupleBuffers inputBuffer){
        //等待input data flag的值为0 表示可以向共享内存传送数据了
        long wstime = System.nanoTime();
        waitInputDataConsumed(shmFlagid);
        long wetime = System.nanoTime();
        for(int i = 0; i < inputBuffer.bufferTypes.length;i++){
            int size = sizes[i];
            switch(inputBuffer.bufferTypes[i]){
                case INT: {
                    putIntToNativeShm(inputShmid[i],inputBuffer.buffers[i].getIntBuffer(),size);
                }
                case FLOAT: {
                    putFloatToNativeShm(inputShmid[i],inputBuffer.buffers[i].getFloatBuffer(),size);
                    break;
                }
                case BOOLEAN: {
                    putBooleanToNativeShm(inputShmid[i],inputBuffer.buffers[i].getBooleanBuffer(),size);
                    break;
                }
                case SHORT: {
                    putShortToNativeShm(inputShmid[i],inputBuffer.buffers[i].getShortBuffer(),size);
                    break;
                }
                case BYTE: {
                    putByteToNativeShm(inputShmid[i],inputBuffer.buffers[i].getByteBuffer(),size);
                    break;
                }
                case DOUBLE: {
                    putDoubleToNativeShm(inputShmid[i],inputBuffer.buffers[i].getDoubleBuffer(),size);
                    break;
                }
                case LONG: {
                    putLongToNativeShm(inputShmid[i],inputBuffer.buffers[i].getLongBuffer(),size);
                    break;
                }
                case CHAR: {
                    putCharToNativeShm(inputShmid[i],inputBuffer.buffers[i].getCharBuffer(),size);
                    break;
                }
                case STRING: {
                    putStringToNativeShm(inputShmid[i], inputBuffer.buffers[i].getStringBuffer(),size);
                    break;
                }
            }
        }
        //设置input flag 为1 表示共享内存的数据已经准备好 内核可以进行计算了
        setInputDataReady(shmFlagid);
        long dtime = System.nanoTime();
        LOG.info("wait for input data Consumed time: " + (wetime-wstime) + ", dataTransferTime: " + (dtime - wetime));
    }

    public void waitAndPollOutputTupleEleFromShm(int[] sizes,TupleBuffers outputBuffers) throws Exception{
        //首先等待outputFlag 的值为1 表示结果可取
        try{
            long wstime = System.nanoTime();
            waitOutputDataReady(shmFlagid);
            long wetime = System.nanoTime();
            outputBuffers.resetBuffers();
            LOG.info("start poll data form native machine");
            for(int i = 0; i <outputBuffers.bufferTypes.length;i++){
                int size = sizes[i];
                switch(outputBuffers.bufferTypes[i]){
                    case INT:{
                        getIntFromNativeShm(outputShmid[i],outputBuffers.buffers[i].getIntBuffer(),size);
                        break;
                    }
                    case BOOLEAN:{
                        getBooleanFromNativeShm(outputShmid[i],outputBuffers.buffers[i].getBooleanBuffer(),size);
                        break;
                    }
                    case SHORT:{
                        getShortFromNativeShm(outputShmid[i],outputBuffers.buffers[i].getShortBuffer(),size);
                        break;
                    }
                    case BYTE:{
                        getByteFromNativeShm(outputShmid[i],outputBuffers.buffers[i].getByteBuffer(),size);
                        break;
                    }
                    case FLOAT:{
                        getFloatFromNativeShm(outputShmid[i],outputBuffers.buffers[i].getFloatBuffer(),size);
                        break;
                    }
                    case DOUBLE:{
                        getDoubleFromNativeShm(outputShmid[i],outputBuffers.buffers[i].getDoubleBuffer(),size);
                        break;
                    }
                    case LONG:{
                        getLongFromNativeShm(outputShmid[i],outputBuffers.buffers[i].getLongBuffer(),size);
                        break;
                    }
                    case CHAR:{
                        getCharFormNativeShm(outputShmid[i],outputBuffers.buffers[i].getCharBuffer(),size);
                        break;
                    }
                    case STRING: {
                        getStringFromNativeShm(outputShmid[i],outputBuffers.buffers[i].getStringBuffer(),size);
                    }
                }
            }
            setOutputDataConsumed(shmFlagid);
            long dtime = System.nanoTime();
            LOG.info("wait for output data time: " + (wetime-wstime) + ", getting data time: "+ (dtime-wetime));
        }catch(Exception e){
            throw e;
        }
    }
    // 该函数返回的是创建的共享内存标识符
    public native int shmGet(int size,int shmTypeFlag);
    // public native void shmClear(int[] shmids);

    public native boolean putIntToNativeShm(int shmid, int[] data,int size);
    public native boolean putLongToNativeShm(int shmid, long[] data,int size);
    public native boolean putShortToNativeShm(int shmid, short[] data, int size);
    public native boolean putByteToNativeShm(int shmid, byte[] data, int size);
    public native boolean putCharToNativeShm(int shmid, char[] data, int size);
    public native boolean putBooleanToNativeShm(int shmid, boolean[] data, int size);
    public native boolean putFloatToNativeShm(int shmid, float[] data, int size);
    public native boolean putDoubleToNativeShm(int shmid, double[] data, int size);
    public native boolean putStringToNativeShm(int shmid, String[] data, int size);

    public native void getIntFromNativeShm(int shmid,int[] data,int size);
    public native void getLongFromNativeShm(int shmid,long[] data, int size);
    public native void getShortFromNativeShm(int shmid,short[] data,int size);
    public native void getByteFromNativeShm(int shmid,byte[] data,int size);
    public native void getCharFormNativeShm(int shmid,char[] data,int size);
    public native void getBooleanFromNativeShm(int shmid,boolean[] data,int size);
    public native void getFloatFromNativeShm(int shmid,float[] data, int size);
    public native void getDoubleFromNativeShm(int shmid,double[] data, int size);
    public native void getStringFromNativeShm(int shmid, String[] data, int size);

    public native void setInputDataReady(int shmid); //将inputFlag的值设置为1 表示输入的数据可用了
    public native void waitOutputDataReady(int shmid);  //阻塞等待ouputFlag的值为1 为1 就返回 不为1 就循环等待
    public native void setOutputDataConsumed(int shmid); // 将outputFlag 设置为0
    public native void setInputDataEnd(int shmid); //设置inputFlag 的值为-1 表示这个bolt要结束了 对应的kernel函数以及相关资源都可以进行清理了
    public native void waitInputDataConsumed(int shmid); //等待共享内存中input 数据被消费 这样就可以传送第二批数据到共享内存了
}
