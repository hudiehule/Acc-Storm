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
    private String[] inputTupleEleTypes;
    private String[] outputTupleEleTypes;
    private BufferManager bufferManager = null;
    private ComponentConnectionToNative connection;
    private ExecutorService threadPool;
   // private WaitingForResults waitingForResultsThread;
   // private AtomicBoolean waiting;
    private long batchCount = 0;
    private long inputCount = 0;
    private List<Tuple> pendings;
    class GettingResultsTask implements Runnable{
        OutputCollector collector;
        long batchStartTime;
        boolean listenning = true;
        List<Tuple> pendings;
        public GettingResultsTask(OutputCollector collector,List<Tuple> tuples){
            this.collector = collector;
            this.pendings = new ArrayList<>(tuples);
        }

        public void setBatchStartTime(long time){
            this.batchStartTime = time;
        }
        public void run(){
            try{
                while(listenning){
                    LOG.info("waiting for result form openclHost");
                    bufferManager.waitAndPollOutputTupleEleFromShm();
                    // long batchNativeTime = System.nanoTime() - batchStartTime;
                    Values[] values = bufferManager.constructOutputData();
                    for(int i = 0;i<values.length;i++){
                        collector.emit(pendings.get(i), values[i]);
                        collector.ack(pendings.get(i));
                    }
                    long batchEndTime = System.nanoTime();
                    LOG.info("batch processing time :" + (batchEndTime - this.batchStartTime));
                }
            }catch (Exception e){
                LOG.info("exception occur :" + e.toString());
                e.printStackTrace();
            }
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


    public BaseRichAccBolt(Class[] inputTupleEleTypes, Class[] outputTupleEleTypes, int batchSize,int tupleParallelism,String kernelFunctionName){ // the sequence of the tupleElements must be corresponding to the sequence of of the kernel function'parameters
        this.batchSize = batchSize;
        this.inputTupleEleTypes = getTypeName(inputTupleEleTypes);
        this.outputTupleEleTypes = getTypeName(outputTupleEleTypes);
        this.kernelFunctionName = kernelFunctionName;
        this.tupleParallelism = tupleParallelism;
    }
    public BaseRichAccBolt(Class[] inputTupleEleTypes, Class[] outputTupleEleTypes,String kernelName){
        this(inputTupleEleTypes,outputTupleEleTypes,DEFAULT_BATCH_SIZE,1,kernelName);
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
        this.bufferManager = new BufferManager(new TupleBuffers(inputTupleEleTypes,batchSize,tupleParallelism),new TupleBuffers(outputTupleEleTypes,batchSize,tupleParallelism));
        this.exeKernelFile = context.getRawTopology().get_kernel_file_str();
        //建立到OpenCL Host端的sock连接
        this.connection = new ComponentConnectionToNative(Utils.getInt(stormConf.get(Config.OCL_NATIVE_PORT)));
        //发送消息给nativeMachine 启动一个program 建立一个FPGA command，并且阻塞等待一个ack返回
        LOG.info("send initial opencl program info");
        connection.sendInitialOpenCLProgramRequest(exeKernelFile,kernelFunctionName,batchSize,tupleParallelism,
                inputTupleEleTypes,bufferManager.getInputBufferShmids(),outputTupleEleTypes,bufferManager.getOutputBufferShmids(),bufferManager.getInputAndOutputFlagShmid());
        LOG.info("get the ack from the native");
        this.pendings = new ArrayList<>(batchSize);
        this.threadPool = Executors.newSingleThreadExecutor();
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
        LOG.info("close the BaseRichAccBolt");
       /* waitingForResultsThread.shutdown(); //关闭线程*/
        threadPool.shutdown();
        cleanpOpenCLProgram(); // 通过设置共享内存中input flag的值为-1 表示这个kernel可以停止运行了 native将会清理资源 包括清理共享内存的资源

        connection.close(); // 关闭socket连接
    }

    @Override
    public void accExecute(Tuple input){
        if(inputCount == batchSize){
            GettingResultsTask getResultTask = new GettingResultsTask(accCollector,pendings);
            getResultTask.setBatchStartTime(System.nanoTime());
            bufferManager.pushInputTuplesFromBufferToShmAndStartKernel();
            threadPool.submit(getResultTask);
            inputCount = 0;
            pendings.clear();
        }
        pendings.add(input);
        bufferManager.putInputTupleToBuffer(input);
        inputCount++;
       /* if(bufferManager.isInputBufferFull()){
            *//*while(!lastBatchFinished){ //当上一批的结果还未返回时 持续进行检查 此处阻塞
            }*//*
            long batchStartTime = System.nanoTime(); // 一个batch开始计算
            LOG.info("start batch " + batchCount++);
            // 将每一个缓冲区的数据发送到共享内存中，发送完成以后将缓冲区清空 将缓冲区的isFull置为false 发送完成以后将共享存储中的inputflag的值设为1 表示数据准备好 kernel可以运行了
            bufferManager.pushInputTuplesFromBufferToShmAndStartKernel(); //如果上一批数据还没被消费 将会等待在这里 阻塞函数
            threadPool.submit(new GettingResultsTask(accCollector,batchStartTime));
        }
        bufferManager.putInputTupleToBuffer(input); //缓冲未满则直接将数据放入缓冲区
        accCollector.ack(input);*/
    }

}
