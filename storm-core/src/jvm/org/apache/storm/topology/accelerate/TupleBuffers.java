package org.apache.storm.topology.accelerate;

import org.apache.storm.tuple.Values;
import sun.swing.BakedArrayList;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2018/5/16.
 */
public class TupleBuffers {
    //元组数据的暂存缓冲 到达一定batch数量便发送给native 内存然后发送给FPGA进行计算 利用泛型 或者反射
    //是否可以考虑用队列作为缓冲 创建一个同步的队列
    class TupleElementBuffer<T>{
        String elementType;
        T[] buffer;
        int N = 0;
        TupleElementBuffer(int size,String type){
            buffer = (T[])new Object[size];
            this.elementType = type;
        }

        int getLength(){
            return N;
        }
        void put(Object value){
            buffer[N++] = (T)value;
        }
        T get(int index){
            return buffer[index];
        }

        void resetN(){
            this.N = 0;
        }
    }

    public TupleElementBuffer[] buffers;
    public boolean isFineGrained;
    public int tupleParallelism;
    public int bufferSize;  // 每一个buffer的大小
    public int batchSize;
    public String[] types;
 //   public int[] sizeBytes; //每个缓冲区的所占内存空间大小 建立共享内存时需要
    public TupleBuffers(String[] tupleEleTypes,int batch,int tupleParallelism){
        this.tupleParallelism = tupleParallelism;
        this.batchSize = batch;
        this.bufferSize = batch * tupleParallelism;
        this.isFineGrained = batchSize == bufferSize ? false : true;
        int elementNum = tupleEleTypes.length;
        buffers = new TupleElementBuffer[elementNum];
        types = new String[elementNum];
        for(int i = 0; i<elementNum;i++){
            String typeName = tupleEleTypes[i];
            switch(typeName){
                case "int": {
                    buffers[i] = new TupleElementBuffer<Integer>(bufferSize,"int");
                    //sizeBytes[i] = size * 4;
                    types[i] = new String("int");
                    break;
                }  //**字节
                case "boolean": {
                    buffers[i] = new TupleElementBuffer<Boolean>(bufferSize,"boolean");
                   // sizeBytes[i] = size * 1;
                    types[i] = new String("boolean");
                    break;
                }
                case "short": {
                    buffers[i] = new TupleElementBuffer<Short>(bufferSize,"short");
                   // sizeBytes[i] = size * 2;
                    types[i] = new String("short");
                    break;
                }
                case "byte": {
                    buffers[i] = new TupleElementBuffer<Byte>(bufferSize,"byte");
                   // sizeBytes[i] = size * 1;
                    types[i] = new String("byte");
                    break; }
                case "float": {
                    buffers[i] = new TupleElementBuffer<Float>(bufferSize,"float");
                   // sizeBytes[i] = size * 4;
                    types[i] = new String("float");
                    break;
                }
                case "double": {
                    buffers[i] = new TupleElementBuffer<Double>(bufferSize,"double");
                   // sizeBytes[i] = size * 8;
                    types[i] = new String("double");
                    break;
                }
                case "long": {
                    buffers[i] = new TupleElementBuffer<Long>(bufferSize,"long");
                    //sizeBytes[i] = size * 8;
                    types[i] = new String("long");
                    break;
                }
                case "char": {
                    buffers[i] = new TupleElementBuffer<Character>(bufferSize,"char");
                    //sizeBytes[i] = size * 1;
                    types[i] = new String("char");
                    break;
                }
            }
        }
    }
    public boolean isFull(){
        return buffers[0].getLength()== bufferSize ? true:false;
    }

    public void resetBuffers(){
        for(int i = 0;i<buffers.length;i++){
            buffers[i].resetN();
        }
    }

    public void addTuple(List<Object> list){
        if(isFineGrained){
            List<Object> objects = null;
             for(int i = 0; i < buffers.length; i++){
                 objects = (List<Object>)list.get(i);
                 for(int j = 0; j < objects.size();j++){
                     buffers[i].put(objects.get(j));
                 }
             }
        }else{
            for(int i = 0; i < buffers.length;i++){
                buffers[i].put(list.get(i));
            }
        }
    }

    public Values[] constructTupleValues(){
        Values[] values = new Values[batchSize];
        if(!isFineGrained){
            for(int i = 0;i < batchSize;i++){
                values[i] = new Values();
                for(int j = 0; j < buffers.length;j++){
                    values[i].add(buffers[j].get(i));
                }
            }
        }else{
            for(int i = 0; i < batchSize;i++){
                values[i] = new Values();
                for(int j = 0;j < buffers.length;j++){
                    List<Object> objs = new ArrayList<>();
                    for(int k = 0; k < tupleParallelism; k++){
                        objs.add(buffers[j].get(i *tupleParallelism + k));
                    }
                    values[i].add(objs);
                }
            }
        }
        return values;
    }

}
