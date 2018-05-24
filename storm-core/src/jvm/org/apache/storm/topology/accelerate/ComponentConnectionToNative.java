package org.apache.storm.topology.accelerate;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;

/**
 * Created by Administrator on 2018/5/15.
 */
public class ComponentConnectionToNative {
    private Socket conn = null; //怎样获取native machine的port
    private BufferedOutputStream out = null;
    private BufferedReader reader = null;
    public boolean isConnected = false;
    public ComponentConnectionToNative(int port){
        try{
            conn = new Socket("localhost",port);conn.setTcpNoDelay(true);
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
    public void startOpenCLProgram(String exeKernelFile,String kernelFunctionName){
        serializeAndSendString("startOpenCL_"+exeKernelFile+"_"+kernelFunctionName);
    }

    /**
     * send a request to OpenCL Host to terminate a program and clean up some resources
     * @param exeKernelFile the file name of the executable kernel file
     * @param kernelFunctionName the kernel function name of this component
     */
    public void cleanupOpenCLProgram(String exeKernelFile,String kernelFunctionName){
        serializeAndSendString("cleanupOpenCL_"+exeKernelFile+"_"+kernelFunctionName);
    }

    /**
     * send a request to start a kernel
     * @param exeKernelFile the executable opencl kernel file
     * @param kernelFunctionName kernel name
     * @param batchSize the batch size of data
     * @param inShmids the shared memory ids of the input data
     * @param outShmids the shared memeory ids of the result
     */
    public void startKernel(String exeKernelFile,String kernelFunctionName,int batchSize,int[] inShmids,int[] outShmids){
        StringBuilder message = new StringBuilder("startKernel_").append(exeKernelFile).append("_").append(kernelFunctionName).append("_").append(String.valueOf(batchSize));
        message.append("$");
        for(int i = 0;i<inShmids.length;i++){
            message.append("_").append(inShmids[i]);
        }
        message.append("$");
        for(int i = 0;i<outShmids.length;i++){
            message.append("_").append(outShmids[i]);
        }
        serializeAndSendString(message.toString());
    }

    public boolean waitingForResult(){
        try {
            while (true) {
                // readLine是一个阻塞函数，当没有数据读取时，就一直会阻塞在那里，而不是返回null,并且只有遇到'/r','/n'或者“/r/n”才会返回
                String recvMsg = reader.readLine();
                if ( recvMsg != "batch finished") {
                    // Connection lost
                    return false;
                }
                return true;
            }
        }catch(Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    private void serializeAndSendString(String msg){
        try{
            byte[] b = msg.getBytes();
            out.write(b);
            out.flush();
        }catch(Exception e){
            e.printStackTrace();
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

