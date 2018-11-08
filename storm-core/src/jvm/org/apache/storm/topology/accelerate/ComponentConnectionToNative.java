package org.apache.storm.topology.accelerate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

/**
 * Created by Administrator on 2018/5/15.
 */
public class ComponentConnectionToNative {
    private static final Logger LOG = LoggerFactory.getLogger(ComponentConnectionToNative.class);
    private Socket conn = null; //怎样获取native machine的port
    private BufferedOutputStream out = null;
    private BufferedReader reader = null;
    public boolean isConnected = false;
    public ComponentConnectionToNative(int port){
        try{
            conn = new Socket("localhost",port);
            conn.setTcpNoDelay(true);
            isConnected = true;
            out = new BufferedOutputStream(conn.getOutputStream());
            reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
          //  LOG.info("Connecting nativeServer at port %d",port);
        }catch(Exception e){
            isConnected = false;
          //  LOG.error(e.getMessage()+" DeviceManager can not connect the native machine!");
        }
    }

    /**
     * send a request to OpenCL Host to start a program
     * @param exeKernelFile the file name of the executable kernel file
     * @param kernelFunctionName the kernel function name of this component
     */
    public void sendInitialOpenCLProgramRequest(String exeKernelFile,String kernelFunctionName,
                                      int batchSize,int tupleParallelism,TupleInnerDataType[] inputDataTypes,int[] inShmids,TupleInnerDataType[] outputDataTypes,int[] outShmids,int shmFlagid){
        String message = Messages.constructStartOpenCLRuntimeMsg(exeKernelFile,kernelFunctionName,batchSize,tupleParallelism,inputDataTypes,inShmids,outputDataTypes,outShmids,shmFlagid);
        try{
            byte[] b = message.getBytes();
            out.write(b);
            out.flush();
        }catch(Exception e){
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }


    public boolean waitingForResult(){
        try {
            // readLine是一个阻塞函数，当没有数据读取时，就一直会阻塞在那里，而不是返回null,并且只有遇到'/r','/n'或者“/r/n”才会返回
            LOG.info("waiting for start opencl runtime ack");
            String recvMsg = reader.readLine();
            if ( !recvMsg.equals(Messages.START_OPENCL_RUNTIME_ACK)) {
                // Connection lost
                return false;
            }
            LOG.info("get the message: " + recvMsg);
            return true;
        }catch(Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    public void close(){
        try{
            reader.close();
            out.close();
            conn.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}

