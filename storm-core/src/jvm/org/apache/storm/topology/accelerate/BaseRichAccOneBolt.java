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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Administrator on 2018/11/22.
 */
public abstract class BaseRichAccOneBolt extends BaseComponent implements IRichAccBolt{
    private static final Logger LOG = LoggerFactory.getLogger(BaseRichAccOneBolt.class);
    private transient OutputCollector accCollector;
    private int tupleParallelism;
    private String exeKernelFile;
    private String kernelFunctionName;
    private TupleInnerDataType[] inputTupleEleTypes;
    private TupleInnerDataType[] outputTupleEleTypes;
    private ConstantParameter[] constantParameters;
    private BufferManager bufferManager = null;
    private ComponentConnectionToNative connection;

    private ExecutorService singleThreadPool;
    public abstract List<Object> getInputTupleValues(Tuple input);

    class GettingResultsTask implements Runnable{
        OutputCollector collector;
        long startTime;
        Tuple waitingForAck;
        public GettingResultsTask(OutputCollector collector,Tuple tuple,long startTime){
            this.collector = collector;
            this.waitingForAck = tuple;
            this.startTime = startTime;
        }

        public void run(){
            try{
                bufferManager.waitAndPollOutputTupleEleFromShm();
                Values values = bufferManager.constructOneOutputData();
                collector.emit(waitingForAck, values);
                collector.ack(waitingForAck);
                waitingForAck = null;
                values = null;
                LOG.info("tuple processing time :" + (System.nanoTime() - this.startTime));
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


    public BaseRichAccOneBolt(TupleInnerDataType[] inputTupleEleTypes, TupleInnerDataType[] outputTupleEleTypes, ConstantParameter[] constantParameters, String kernelFunctionName,int tupleParallelism){ // the sequence of the tupleElements must be corresponding to the sequence of of the kernel function'parameters
        this.inputTupleEleTypes = inputTupleEleTypes;
        this.outputTupleEleTypes = outputTupleEleTypes;
        this.constantParameters = constantParameters;
        this.kernelFunctionName = kernelFunctionName;
        this.tupleParallelism = tupleParallelism;
    }
    public BaseRichAccOneBolt(TupleInnerDataType[] inputTupleEleTypes, TupleInnerDataType[] outputTupleEleTypes,ConstantParameter[] constantParameters,String kernelName){
        this(inputTupleEleTypes,outputTupleEleTypes,constantParameters,kernelName,1);
    }


    public BaseRichAccOneBolt(TupleInnerDataType[] inputTupleEleTypes, TupleInnerDataType[] outputTupleEleTypes, String kernelName,int tupleParallelism){
        this(inputTupleEleTypes,outputTupleEleTypes,null,kernelName,tupleParallelism);
    }

    public BaseRichAccOneBolt(TupleInnerDataType[] inputTupleEleTypes, TupleInnerDataType[] outputTupleEleTypes, String kernelName){
        this(inputTupleEleTypes,outputTupleEleTypes,null,kernelName,1);
    }

    /**
     * 当这个bolt被调度在FPGA上执行时，执行prepareOpenCL
     * @param stormConf
     * @param context
     */
    @Override
    public void accPrepare(Map stormConf, TopologyContext context, OutputCollector collector){
        this.accCollector = collector;
        //建立输入输出缓冲区与buffermanager 同时也建立了共享内存
        this.bufferManager = new BufferManager(new TupleBuffers(inputTupleEleTypes,1),new TupleBuffers(outputTupleEleTypes,1));
        this.exeKernelFile = context.getRawTopology().get_kernel_file_str();
        //建立到OpenCL Host端的sock连接
        this.connection = new ComponentConnectionToNative(Utils.getInt(stormConf.get(Config.OCL_NATIVE_PORT)));
        //发送消息给nativeMachine 启动一个program 建立一个FPGA command，并且阻塞等待一个ack返回
        LOG.info("send initial opencl program info");
        connection.sendInitialOpenCLProgramRequest(exeKernelFile,kernelFunctionName,1,tupleParallelism,
                inputTupleEleTypes,bufferManager.getInputBufferShmids(),outputTupleEleTypes,bufferManager.getOutputBufferShmids(),bufferManager.getInputAndOutputFlagShmid(),constantParameters);
        LOG.info("get the ack from the native");
        this.singleThreadPool = Executors.newSingleThreadExecutor();
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
        long receiveTime = System.nanoTime();
        bufferManager.putInputTuplesDirectlyToShmAndStartKernel(getInputTupleValues(input));
        singleThreadPool.submit(new GettingResultsTask(this.accCollector,input,receiveTime));
    }

}

