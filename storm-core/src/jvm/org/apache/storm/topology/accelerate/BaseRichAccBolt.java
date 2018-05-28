package org.apache.storm.topology.accelerate;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseComponent;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;


import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * BaseRichAccBolt doesn't contain computing logic but some work including receiving tuples form upStream, extract the specific information about the tuple,
 * buffer these data,when the buffer num reaching a set value ,then send these data to the FPGA devices througn the native method
 * Created by Administrator on 2018/1/15.
 */

public abstract class BaseRichAccBolt extends BaseComponent implements IRichAccBolt{
    private static int DEFAULT_BATCH_SIZE = 200;
    private int batchSize;
    private String exeKernelFile;
    private String kernelFunctionName;
 /*   private TupleBuffers inputBuffers;
    private TupleBuffers outputBuffers;*/
    private Class[] inputTupleEleTypes;
    private Class[] outputTupleEleTypes;
    private BufferManager bufferManager = null;
    private volatile boolean lastBatchFinished = true;
    private ComponentConnectionToNative connection;
    private WaitingForResults waitingForResultsThread;
    private AtomicBoolean waiting;
    class WaitingForResults extends Thread{
        OutputCollector collector;
        ComponentConnectionToNative conn; // 需要一个socket连接
        boolean listenning;
        long batchStartTime;
        public WaitingForResults(OutputCollector collector,ComponentConnectionToNative connection){
            this.collector = collector;
            this.conn = connection;
            this.listenning = true;
        }
        public void run(){
            while(listenning){
                if(waiting.get() == true){
                    boolean batchResultReturned = conn.waitingForResult(); // 阻塞函数等待这一批tuple计算完成
                    if(batchResultReturned){
                        bufferManager.pollOutputTupleEleFromShm();
                        long batchNativeTime = System.nanoTime() - batchStartTime;
                        Values[] values = bufferManager.constructOutputData();
                        for(int i = 0;i<values.length;i++){
                            collector.emit(values[i]);
                        }
                        lastBatchFinished = true;
                    }
                    waiting.compareAndSet(true,false);
                }
            }
        }

        public void running(long startTime){
            batchStartTime = startTime;
            waiting.compareAndSet(false,true);
        }
        public void shutdown(){
            this.listenning = false;
        }
    }

    public BaseRichAccBolt(Class[] inputTupleEleTypes, Class[] outputTupleEleTypes, int batchSize,String kernelFunctionName){ // the sequence of the tupleElements must be corresponding to the sequence of of the kernel function'parameters
        this.batchSize = batchSize;
        this.inputTupleEleTypes = inputTupleEleTypes;
        this.outputTupleEleTypes = outputTupleEleTypes;
        this.kernelFunctionName = kernelFunctionName;
    }
    public BaseRichAccBolt(Class[] inputTupleEleTypes, Class[] outputTupleEleTypes,String kernelName){
        this(inputTupleEleTypes,outputTupleEleTypes,DEFAULT_BATCH_SIZE,kernelName);
    }


    /**
     * 当这个bolt被调度在FPGA上执行时，执行prepareOpenCL
     * @param stormConf
     * @param context
     */
    @Override
    public void accPrepare(Map stormConf, TopologyContext context,OutputCollector collector){
        //建立输入输出缓冲区与buffermanager
        this.bufferManager = new BufferManager(new TupleBuffers(inputTupleEleTypes,batchSize),new TupleBuffers(outputTupleEleTypes,batchSize));
        String exeKernelFile = context.getRawTopology().get_kernel_file_str();
        this.exeKernelFile = exeKernelFile;
        //建立到OpenCL Host端的sock连接
        this.connection = new ComponentConnectionToNative(Utils.getInt(stormConf.get(Config.OCL_NATIVE_PORT)));
        //发送消息给nativeMachine 启动一个program 建立一个FPGA command，
        connection.startOpenCLProgram(exeKernelFile,kernelFunctionName);
        this.waitingForResultsThread = new WaitingForResults(collector,connection);
        waitingForResultsThread.setName("waitingForResultsThread");
        this.waiting = new AtomicBoolean(false);
        waitingForResultsThread.start();
    }


    @Override
    public void cleanup() {

    }

    @Override
    public void accCleanup(){
        //发送消息给nativeMachine  将这个bolt对应的FPGA 的opencl资源都清除
        connection.cleanupOpenCLProgram(exeKernelFile,kernelFunctionName);
        waitingForResultsThread.shutdown(); //关闭线程
        connection.close(); // 关闭socket连接
        bufferManager.clearShm(); // 清理共享内存
    }

    @Override
    public void accExecute(Tuple input){
        if(bufferManager.isInputBufferFull()){
            while(!lastBatchFinished){ //当上一批的结果还未返回时 持续进行检查 此处阻塞
            }
            long batchStartTime = System.nanoTime(); // 一个batch开始计算
            // 将每一个缓冲区的数据发送到共享内存中，发送完成以后将缓冲区清空 将缓冲区的isFull置为false
            bufferManager.pushInputTuplesFromBufferToShm();

            //socket立即发送信息给OpenCL Host可以进行kernel计算 lastBatchFinished置为false 发送以后立即返回
            // 此时有线程等待OpenCL Host将结果回传给这个executor 传回以后这个线程使用collector.emit一条条发送给下游，完成以后将lastBatchFinished置为true
            connection.startKernel(exeKernelFile,kernelFunctionName,batchSize,bufferManager.getInputBufferShmids(),bufferManager.getOutputBufferShmids());
            lastBatchFinished = false;
            // 设置waiting唤醒waitingForResult线程继续执行 等待OpenCL执行完kernel将结果传回来 并组装成tuple发送到下游
            waitingForResultsThread.running(batchStartTime);

            /*
            在发送之前先检查上一批是否结果已经返回，如果已经返回，利用jni将数据发送到native共享内存；返回后表示数据发送成功 立即将缓冲区清空 可以接收下一批数据
            如果没有返回 则等待上一批数据处理完；
            */
            /*
            给每一个accbolt创建一个connection到openclHost 利用它来发送和接收控制信息 发送和接收数据
            利用信号通知相应的线程去处理下面的操作
            将这个batch通过jni放到native内存中；然后清空buffers 发送消息给opencl host去取数据并启动kernel进行计算 这个过程中应该还可以继续将数据放入缓冲区
            一旦这个batch的结果返回，则可以直接将这个刚缓冲的数据发送到native然后继续进行处理 类似CPU的流水线技术 所以这里是否要建立双缓冲
            等待计算结果？阻塞？不应该阻塞？
             */
        }
        bufferManager.putInputTupleToBuffer(input); //缓冲未满则直接将数据放入缓冲区
    }

}
