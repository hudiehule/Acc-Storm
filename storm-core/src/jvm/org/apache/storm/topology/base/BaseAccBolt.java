package org.apache.storm.topology.base;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IAccBolt;
import org.apache.storm.tuple.Tuple;

import java.util.*;

/**
 * BaseAccBolt doesn't contain computing logic but some work including receiving tuples form upStream, extract the specific information about the tuple,
 * buffer these data,when the buffer num reaching a seted value ,them send these data to the FPGA devices througn the native method
 * Created by Administrator on 2018/1/15.
 */

public abstract class BaseAccBolt extends BaseComponent implements IAccBolt{
    //元组数据的暂存缓冲 到达一定batch数量便发送给native 内存然后发送给FPGA进行计算 利用泛型 或者反射
    //是否可以考虑用队列作为缓冲 创建一个同步的队列
    private int batchSize;
    private class TupleElementBuffer<T>{
        T[] buffer;
        int N;
        TupleElementBuffer(int size){
            buffer = (T[])new Object[size];
        }

        synchronized void put(Object value){
            buffer[N++] = (T)value;
        }
    }

    private TupleElementBuffer[] buffers;
    public BaseAccBolt(Class[] tupleEleTypes,int batchSize){ // the sequence of the tupleElements must be corresponding to the sequence of of the kernel function'parameters
        this.batchSize = batchSize;
        int elementNum = tupleEleTypes.length;
        buffers = new TupleElementBuffer[elementNum];
        for(int i = 0; i<elementNum;i++){
            String typeName = tupleEleTypes[i].getSimpleName().toLowerCase();
            switch(typeName){
                case "int": { buffers[i] = new TupleElementBuffer<Integer>(batchSize*2); break; }
                case "boolean": { buffers[i] = new TupleElementBuffer<Boolean>(batchSize*2); break; }
                case "short": { buffers[i] = new TupleElementBuffer<Short>(batchSize*2); break; }
                case "byte": { buffers[i] = new TupleElementBuffer<Byte>(batchSize*2); break; }
                case "float": { buffers[i] = new TupleElementBuffer<Float>(batchSize*2); break; }
                case "double": { buffers[i] = new TupleElementBuffer<Double>(batchSize*2); break; }
                case "long": { buffers[i] = new TupleElementBuffer<Long>(batchSize*2); break; }
                case "char": { buffers[i] = new TupleElementBuffer<Character>(batchSize*2); break; }
            }
        }
    }
    public BaseAccBolt(Class[] tupleEleTypes){
        this(tupleEleTypes,100);
    }
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void cleanup() {
    }

    //按照顺序提取tuple里面的原始数据类型信息
    public abstract Class[] extractTupleElements(Tuple tuple); //用户实现  将tuple分离出由基本类型数据组成的一些元素的集合
    @Override
    public void accExecute(Tuple input){
        /*
        对到来的每一个tuple 分离出它其中所包含的所有待处理的数据信息，这些信息可能是一个int型变量 一个double型的变量
        使用batcher 将其放入global 内存中 tuple的个数达到设定值以后 将这一批tuple
        发送给FPGA进行处理
       */

    }

}
