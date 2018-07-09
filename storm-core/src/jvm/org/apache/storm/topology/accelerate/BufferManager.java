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
    public BufferManager(TupleBuffers inputBuffer,TupleBuffers outputBuffer){
        this.inputBuffer = inputBuffer;
        this.outputBuffer = outputBuffer;
        this.nativeBufferManager = new NativeBufferManager();
        //  建立native共享内存 对input和output都要建立 并且建立一个共享存储区 存放flag变量
        //  NativeBufferManager.shmGet(inputBuffer.size);
        initialShm();
    }

    /**
     * initially create the shared memory
     */
    private void initialShm(){
       // generateShmKeys(inputBufferShmKeys,outputBufferShmKeys);
        nativeBufferManager.crateSharedMemory(inputBuffer.size,inputBuffer.types,outputBuffer.types);
        nativeBufferManager.createInputAndOutputFlagShm();
    }

  /*  public void clearShm(){
        nativeBufferManager.clearShareMemory();
    }*/
    public boolean isInputBufferFull(){
        return inputBuffer.isFull();
    }

    public int[] getInputBufferShmids(){
        return nativeBufferManager.getInputShmids();
    }
    public int[] getOutputBufferShmids(){
        return nativeBufferManager.getOutputShmids();
    }
    public int getInputAndOutputFlagShmid(){
        return nativeBufferManager.getShmFlagid();
    }
    public void putInputTupleToBuffer(Tuple tuple){
         List<Object> tupleElements = tuple.getValues();
         inputBuffer.addTuple(tupleElements);
    }

    public void waitAndPollOutputTupleEleFromShm(){
         nativeBufferManager.waitAndPollOutputTupleEleFromShm(outputBuffer.size,outputBuffer);
    }

    public void pushInputTuplesFromBufferToShmAndStartKernel(){
         nativeBufferManager.pushInputTuplesFromBufferToShmAndStartKernel(inputBuffer.size,inputBuffer);
         inputBuffer.resetBuffers();
    }

    public void setKernelInputFlagEnd(){
        nativeBufferManager.setKernelInputFlagEnd();
    }
    /**
     * construct the tuples that will be sent to the downstream
     * @return
     */
    public Values[] constructOutputData(){
        return outputBuffer.constructTupleValues();
    }

}
