package org.apache.storm.topology.accelerate;

import org.apache.storm.tuple.Values;
import java.util.List;

/**
 * Created by Administrator on 2018/5/16.
 */
public class TupleBuffers {
    //元组数据的暂存缓冲 到达一定batch数量便发送给native 内存然后发送给FPGA进行计算 利用泛型 或者反射
    //是否可以考虑用队列作为缓冲 创建一个同步的队列
    public TupleElementBuffer[] buffers;
    public int batchSize;
    public DataType[] bufferTypes;
    public boolean[] isArrays;
    public int[] bufferSizes;
    public TupleBuffers(TupleInnerDataType[] tupleEleTypes, int batch){
        this.batchSize = batch;
        int dataTypeNum = tupleEleTypes.length;
        bufferTypes = new DataType[dataTypeNum];
        isArrays = new boolean[dataTypeNum];
        bufferSizes = new int[dataTypeNum];
        buffers = new TupleElementBuffer[dataTypeNum];
        DataType dataType;
        boolean isArray;
        int arraySize;
        for(int i = 0; i<dataTypeNum;i++){
            dataType = tupleEleTypes[i].type;
            isArray= tupleEleTypes[i].isArray;
            arraySize = tupleEleTypes[i].arraySize;
            isArrays[i] = isArray;
            bufferTypes[i] = dataType;
            bufferSizes[i] = arraySize * batchSize;
            buffers[i] = new TupleElementBuffer(dataType, isArray, batchSize, arraySize);
        }
    }
    public boolean isFull(){
        return buffers[0].getLength()== buffers[0].bufferSize? true:false;
    }

    public void resetBuffers(){
        for(int i = 0;i<buffers.length;i++){
            buffers[i].resetN();
        }
    }

    public void putTupleValues(List<Object> list){
        for(int i = 0; i < buffers.length;i++){
            buffers[i].put(list.get(i));
        }
    }

    public Values[] constructTupleValues(){
        Values[] values = new Values[batchSize];
        for(int i = 0;i < batchSize;i++){
            values[i] = new Values();
            for(int j = 0; j < buffers.length;j++){
                /*if(isArrays[j]){
                    values[i].add(buffers[j].getArray(i));
                }else{
                    values[i].add(buffers[j].get(i));
                }*/
                buffers[j].putDataIntoValues(values[i],i);
            }
        }
        return values;
    }

}
