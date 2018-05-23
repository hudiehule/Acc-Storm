package org.apache.storm.topology.accelerate;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.List;

/**
 * Created by Administrator on 2018/5/16.
 */
public class BufferManager {
    private NativeBufferManager nativeBufferManager;
    private TupleBuffers inputBuffer = null;
    private TupleBuffers outputBuffer = null;
    // private int[] inputBufferShmids;
    private int[] inputBufferShmKeys; //保存每个inputBuffer对应的共享内存数组的key 需要传送给Native OpenCL Host端
    private int[] outputBufferShmKeys;
    public BufferManager(TupleBuffers inputBuffer,TupleBuffers outputBuffer){
        this.inputBuffer = inputBuffer;
        this.outputBuffer = outputBuffer;
        this.nativeBufferManager = new NativeBufferManager();
        inputBufferShmKeys = new int[inputBuffer.size];
        outputBufferShmKeys = new int[outputBuffer.size];
        //  建立native共享内存 对input和output都要建立
        //  NativeBufferManager.shmGet(inputBuffer.size);
        initialShm();
    }

    /**
     * 为每个共享内存数组生成一个key，它是shmget函数的第一个参数
     * @param inputKeyArr
     * @param outputKeyArr
     */
    private void generateShmKeys(int[] inputKeyArr,int[] outputKeyArr){

    }

    /**
     * initially create the shared memory
     */
    private void initialShm(){
        generateShmKeys(inputBufferShmKeys,outputBufferShmKeys);
        nativeBufferManager.crateSharedMemory(inputBuffer.size,inputBufferShmKeys,inputBuffer.types,outputBufferShmKeys,outputBuffer.types);
    }

    public void clearShm(){
        nativeBufferManager.clearShareMemory();
    }
    public boolean isInputBufferFull(){
        return inputBuffer.isFull();
    }

    public int[] getInputBufferShmKeys(){
        return inputBufferShmKeys;
    }
    public int[] getOutputBufferShmKeys(){
        return outputBufferShmKeys;
    }
    public void putInputTupleToBuffer(Tuple tuple){
         List<Object> tupleElements = tuple.getValues();
         inputBuffer.addTuple(tupleElements);
    }

    public void pollOutputTupleEleFromShm(){
         nativeBufferManager.pollOutputTupleEleFromShm(outputBuffer.size,outputBuffer);
    }

    public void pushInputTuplesFromBufferToShm(){
         nativeBufferManager.pushInputTuplesFromBufferToShm(inputBuffer.size,inputBuffer);
         inputBuffer.resetBuffers();
    }

    /**
     * construct the tuples that will be sent to the downstream
     * @return
     */
    public Values[] constructOutputData(){
        return outputBuffer.constructTupleValues();
    }

}
