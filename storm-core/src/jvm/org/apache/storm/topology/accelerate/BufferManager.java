package org.apache.storm.topology.accelerate;

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
        initialShm();
    }

    /**
     * initially create the shared memory
     */
    private void initialShm(){
       // generateShmKeys(inputBufferShmKeys,outputBufferShmKeys);
        nativeBufferManager.crateSharedMemory(inputBuffer.bufferSizes,inputBuffer.bufferTypes,outputBuffer.bufferSizes,outputBuffer.bufferTypes);
        nativeBufferManager.createInputAndOutputFlagShm();
    }


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
    public void putInputTupleValuesToBuffer(List<Object> tupleValues){
         inputBuffer.putTupleValues(tupleValues);
    }

    public void waitAndPollOutputTupleEleFromShm() throws Exception{
        try{
            nativeBufferManager.waitAndPollOutputTupleEleFromShm(outputBuffer.bufferSizes,outputBuffer);
        }catch(Exception e){
            throw e;
        }
    }

    public void pushInputTuplesFromBufferToShmAndStartKernel(){
         nativeBufferManager.pushInputTuplesFromBufferToShmAndStartKernel(inputBuffer.bufferSizes,inputBuffer);
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

    public void putInputTuplesDirectlyToShmAndStartKernel(List<Object> values){
        inputBuffer.putTupleValues(values);
        nativeBufferManager.pushInputTuplesFromBufferToShmAndStartKernel(inputBuffer.bufferSizes,inputBuffer);
    }

    public Values constructOneOutputData(){
        Values[] elements = constructOutputData();
        return elements[0];
    }
}
