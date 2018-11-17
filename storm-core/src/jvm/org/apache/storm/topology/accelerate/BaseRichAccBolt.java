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
    private int tupleParallelism;
    private String exeKernelFile;
    private String kernelFunctionName;
    private TupleInnerDataType[] inputTupleEleTypes;
    private TupleInnerDataType[] outputTupleEleTypes;
    private BufferManager bufferManager = null;
    private ComponentConnectionToNative connection;

    private ExecutorService singleThreadPool;
    // private WaitingForResults waitingForResultsThread;
   // private AtomicBoolean waiting;
    private ArrayList<Tuple> pendings;
    private int count = 0;
    public abstract List<Object> getInputTupleValues(Tuple input);
    /*class WaitingForResults extends Thread{
        OutputCollector collector;
        volatile boolean listenning;
        Queue<Long> batchStartTimeQueue;
        Tuple temp;
        Values[] values;
        public WaitingForResults(OutputCollector collector){
            this.collector = collector;
            this.listenning = true;
            batchStartTimeQueue = new LinkedList<>();
        }
        public void run(){
            try{
                while(listenning){
                    LOG.info("waiting for result form OpenCLHost");
                    bufferManager.waitAndPollOutputTupleEleFromShm();
                    values = bufferManager.constructOutputData();
                    for(int i = 0;i<values.length;i++){
                        temp = pendings.poll();
                        collector.emit(temp,values[i]);
                        collector.ack(temp);
                        temp = null;
                    }
                    LOG.info("get result from openclHost,batchTime: " + (System.nanoTime() - batchStartTimeQueue.poll()));
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
    }*/

    class GettingResultsTask implements Runnable{
        OutputCollector collector;
        long batchStartTime;
        List<Tuple> waitingForAck = new ArrayList<>(batchSize);
        public GettingResultsTask(OutputCollector collector,List<Tuple> tuples,long startTime){
            this.collector = collector;
            this.waitingForAck.addAll(tuples);
            this.batchStartTime = startTime;
        }

        public void run(){
            try{
                LOG.info("waiting for result form openclHost");
                bufferManager.waitAndPollOutputTupleEleFromShm();
                // long batchNativeTime = System.nanoTime() - batchStartTime;
                Values[] values = bufferManager.constructOutputData();
                for(int i = 0;i<values.length;i++){
                    collector.emit(waitingForAck.get(i), values[i]);
                    collector.ack(waitingForAck.get(i));
                }
                waitingForAck.clear();
                waitingForAck = null;
                values = null;
                LOG.info("batch processing time :" + (System.nanoTime() - this.batchStartTime));
                collector = null;
            }catch (Exception e){
                LOG.info("exception occur :" + e.toString());
                e.printStackTrace();
            }
        }
    }


    public void cleanupOpenCLProgram(){
        bufferManager.setKernelInputFlagEnd();
    }


    public BaseRichAccBolt(TupleInnerDataType[] inputTupleEleTypes, TupleInnerDataType[] outputTupleEleTypes, int batchSize,String kernelFunctionName,int tupleParallelism){ // the sequence of the tupleElements must be corresponding to the sequence of of the kernel function'parameters
        this.batchSize = batchSize;
        this.inputTupleEleTypes = inputTupleEleTypes;
        this.outputTupleEleTypes = outputTupleEleTypes;
        this.kernelFunctionName = kernelFunctionName;
        this.tupleParallelism = tupleParallelism;
        this.pendings = new ArrayList<>(batchSize);
    }
    public BaseRichAccBolt(TupleInnerDataType[] inputTupleEleTypes, TupleInnerDataType[] outputTupleEleTypes,String kernelName){
        this(inputTupleEleTypes,outputTupleEleTypes,DEFAULT_BATCH_SIZE,kernelName,1);
    }

    public BaseRichAccBolt(TupleInnerDataType[] inputTupleEleTypes, TupleInnerDataType[] outputTupleEleTypes,int batchSize, String kernelName){
        this(inputTupleEleTypes,outputTupleEleTypes,batchSize,kernelName,1);
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
        connection.sendInitialOpenCLProgramRequest(exeKernelFile,kernelFunctionName,batchSize,tupleParallelism,
                inputTupleEleTypes,bufferManager.getInputBufferShmids(),outputTupleEleTypes,bufferManager.getOutputBufferShmids(),bufferManager.getInputAndOutputFlagShmid());
        LOG.info("get the ack from the native");
        this.singleThreadPool = Executors.newSingleThreadExecutor();
        /*this.waitingForResultsThread = new WaitingForResults(collector);
        waitingForResultsThread.setName("waitingForResultsThread");
        //  this.waiting = new AtomicBoolean(false);
        waitingForResultsThread.start();*/
        /*this.waitingForResultsThread = new WaitingForResults(collector);
        waitingForResultsThread.setName("waitingForResultsThread");
        //this.waiting = new AtomicBoolean(false);
        waitingForResultsThread.start();*/
        LOG.info("acc prepare accomplished");
    }


    @Override
    public void cleanup() {

    }

    @Override
    public void accCleanup(){
        //发送消息给nativeMachine  将这个bolt对应的FPGA 的opencl资源都清除
      //  connection.cleanupOpenCLProgram(exeKernelFile,kernelFunctionName);
        cleanup();
        LOG.info("close the BaseRichAccBolt");
        singleThreadPool.shutdown(); //关闭线程
        cleanupOpenCLProgram(); // 通过设置共享内存中input flag的值为-1 表示这个kernel可以停止运行了 native将会清理资源 包括清理共享内存的资源

        connection.close(); // 关闭socket连接
    }

    @Override
    public void accExecute(Tuple input){
        if(count == batchSize){
          //  LOG.info("the pending size is : " + pendings.size()); // 一个batch开始计算
            // 将每一个缓冲区的数据发送到共享内存中，发送完成以后将缓冲区清空 将缓冲区的isFull置为false 发送完成以后将共享存储中的inputflag的值设为1 表示数据准备好 kernel可以运行了
            //waitingForResultsThread.batchStartTimeQueue.offer(System.nanoTime());
            bufferManager.pushInputTuplesFromBufferToShmAndStartKernel(); //如果上一批数据还没被消费 将会等待在这里 阻塞函数
            singleThreadPool.submit(new GettingResultsTask(this.accCollector,pendings,System.nanoTime()));
            pendings.clear();
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
            count = 0;
        }
        bufferManager.putInputTupleValuesToBuffer(getInputTupleValues(input)); //缓冲未满则直接将数据放入缓冲区
        //这里不能直接ack 放入到一个队列中，等待结果回传以后再ack
        count++;
        pendings.add(input);
    }

}
