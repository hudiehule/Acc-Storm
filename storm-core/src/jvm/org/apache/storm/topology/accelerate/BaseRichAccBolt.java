package org.apache.storm.topology.accelerate;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseComponent;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * BaseRichAccBolt doesn't contain computing logic but some work including receiving tuples form upStream, extract the specific information about the tuple,
 * buffer these data,when the buffer num reaching a set value ,then send these data to the FPGA devices througn the native method
 * Created by Administrator on 2018/1/15.
 */

public abstract class BaseRichAccBolt extends BaseComponent implements IRichAccBolt{
    private static final Logger LOG = LoggerFactory.getLogger(BaseRichAccBolt.class);
    private transient OutputCollector accCollector;
    private static int DEFAULT_BATCH_SIZE = 200;
    private int batchSize;
    private String exeKernelFile;
    private String kernelFunctionName;
    private String[] inputTupleEleTypes;
    private String[] outputTupleEleTypes;
    private BufferManager bufferManager = null;
 //   private volatile boolean lastBatchFinished = true;
    private ComponentConnectionToNative connection;
 //   private ExecutorService threadPool;
    private WaitingForResults waitingForResultsThread;
    private TestThread testThread;
    private AtomicBoolean waiting;
    private long batchCount = 0;
    /*class GettingResultsTask implements Runnable{
        *//*private long batchStartTime;
        public GettingResultsTask(long startTime){
            batchStartTime = startTime;
        }*//*
        @Override
        public void run() {
            while(true){
                System.out.println("waiting for result form openclHost");
                bufferManager.waitAndPollOutputTupleEleFromShm();
              //  long batchNativeTime = System.nanoTime() - batchStartTime;
                Values[] values = bufferManager.constructOutputData();
                for(int i = 0;i<values.length;i++){
                    accCollector.emit(values[i]);
                }
                System.out.println("get result from openclHost");
            }
        }
    }*/
    class TestThread extends Thread{
        private volatile boolean cancel = false;
        @Override
        public void run() {
            while(!cancel){
                Utils.sleep(2000);
                LOG.info("test Thread");
            }
        }
        public void shutdown(){
            cancel = true;
        }
    }
    class WaitingForResults extends Thread{
        OutputCollector collector;
        volatile boolean listenning;
        //long batchStartTime;
        public WaitingForResults(OutputCollector collector){
            this.collector = collector;
            this.listenning = true;
        }
        public void run(){
            try{
                while(listenning){
                    LOG.info("waiting for result form openclHost");
                    bufferManager.waitAndPollOutputTupleEleFromShm();
                    // long batchNativeTime = System.nanoTime() - batchStartTime;
                    Values[] values = bufferManager.constructOutputData();
                    for(int i = 0;i<values.length;i++){
                        collector.emit(values[i]);
                    }
                    LOG.info("get result from openclHost");
                }
            }catch (Exception e){
                LOG.info("exception occur :" + e.toString());
                e.printStackTrace();
            }
        }

        public void shutdown(){
            LOG.info("the state of the thread: " + this.getState());
            LOG.info("listening = " + this.listenning);
            this.listenning = false;
        }
    }

    public String[] getTypeName(Class[] dataTypes){
        String[] names = new String[dataTypes.length];
        for(int i = 0; i<dataTypes.length;i++){
            names[i] = dataTypes[i].getSimpleName().toLowerCase();
        }
        return names;
    }

    public void cleanpOpenCLProgram(){
        bufferManager.setKernelInputFlagEnd();
    }


    public BaseRichAccBolt(Class[] inputTupleEleTypes, Class[] outputTupleEleTypes, int batchSize,String kernelFunctionName){ // the sequence of the tupleElements must be corresponding to the sequence of of the kernel function'parameters
        this.batchSize = batchSize;
        this.inputTupleEleTypes = getTypeName(inputTupleEleTypes);
        this.outputTupleEleTypes = getTypeName(outputTupleEleTypes);
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
        this.accCollector = collector;
        //建立输入输出缓冲区与buffermanager 同时也建立了共享内存
        this.bufferManager = new BufferManager(new TupleBuffers(inputTupleEleTypes,batchSize),new TupleBuffers(outputTupleEleTypes,batchSize));
        this.exeKernelFile = context.getRawTopology().get_kernel_file_str();
        //建立到OpenCL Host端的sock连接
        this.connection = new ComponentConnectionToNative(Utils.getInt(stormConf.get(Config.OCL_NATIVE_PORT)));
        //发送消息给nativeMachine 启动一个program 建立一个FPGA command，并且阻塞等待一个ack返回
        LOG.info("send initial opencl program info");
        connection.sendInitialOpenCLProgramRequest(exeKernelFile,kernelFunctionName,batchSize,
                inputTupleEleTypes,bufferManager.getInputBufferShmids(),outputTupleEleTypes,bufferManager.getOutputBufferShmids(),bufferManager.getInputAndOutputFlagShmid());
        LOG.info("get the ack from the native");
       /* this.threadPool = Executors.newSingleThreadExecutor();
        threadPool.execute(new GettingResultsTask());*/
        this.waitingForResultsThread = new WaitingForResults(collector);
        waitingForResultsThread.setName("waitingForResultsThread");
      //  this.waiting = new AtomicBoolean(false);
        waitingForResultsThread.start();
        this.testThread = new TestThread();
        testThread.setName("hudie-test-thread");
        testThread.start();
        LOG.info("acc prepare accomplished");
    }


    @Override
    public void cleanup() {
        LOG.info("close acc bolt");
    }

    @Override
    public void accCleanup(){
        //发送消息给nativeMachine  将这个bolt对应的FPGA 的opencl资源都清除
      //  connection.cleanupOpenCLProgram(exeKernelFile,kernelFunctionName);
        LOG.info("close the BaseRichAccBolt");
        cleanpOpenCLProgram(); // 通过设置共享内存中input flag的值为-1 表示这个kernel可以停止运行了 native将会清理资源 包括清理共享内存的资源
      //  threadPool.shutdown();
        LOG.info("waiting for result thread state: "+waitingForResultsThread.getState());
        LOG.info("waiting for result thread is alive: "+waitingForResultsThread.isAlive());
        LOG.info("waiting for result thread is interrupted: "+waitingForResultsThread.isInterrupted());
        LOG.info("thread stack element:");
        for(StackTraceElement ele : waitingForResultsThread.getStackTrace()){
            LOG.info(ele.toString());
        }
        waitingForResultsThread.shutdown(); //关闭线程
        connection.close(); // 关闭socket连接
    }

    @Override
    public void accExecute(Tuple input){
        if(bufferManager.isInputBufferFull()){
            /*while(!lastBatchFinished){ //当上一批的结果还未返回时 持续进行检查 此处阻塞
            }*/
            long batchStartTime = System.nanoTime(); // 一个batch开始计算
            LOG.info("start batch " + batchCount++);
            // 将每一个缓冲区的数据发送到共享内存中，发送完成以后将缓冲区清空 将缓冲区的isFull置为false 发送完成以后将共享存储中的inputflag的值设为1 表示数据准备好 kernel可以运行了
            bufferManager.pushInputTuplesFromBufferToShmAndStartKernel(); //如果上一批数据还没被消费 将会等待在这里 阻塞函数

            LOG.info("waiting for result thread state: "+waitingForResultsThread.getState());
            LOG.info("waiting for result thread is alive: "+waitingForResultsThread.isAlive());
            LOG.info("waiting for result thread is interrupted: "+waitingForResultsThread.isInterrupted());
            LOG.info("thread stack element:");
            for(StackTraceElement ele : waitingForResultsThread.getStackTrace()){
                LOG.info(ele.toString());
            }

            LOG.info("test Thread state: "+testThread.getState());
            LOG.info("test Thread is alive: "+testThread.isAlive());
            LOG.info("test Thread is interrupted: "+testThread.isInterrupted());
            LOG.info("test Thread stack element:");
            for(StackTraceElement ele : testThread.getStackTrace()){
                LOG.info(ele.toString());
            }
            // 此时有线程等待OpenCL Host将结果回传给这个executor 传回以后这个线程使用collector.emit一条条发送给下游，完成以后将lastBatchFinished置为true

            // 设置waiting为true唤醒waitingForResult线程继续执行 等待OpenCL执行完kernel将结果传回来 并组装成tuple发送到下游
            // waitingForResultsThread.setBatchStartTime(batchStartTime);
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
        accCollector.ack(input);
    }

}
