package org.apache.storm.topology.accelerate;

/**
 * Created by Administrator on 2018/5/31.
 */
public class Messages {
    /**
     * Messages send to native
     */
    public static final String INITIAL_OPENCL_PROGRAM = "INITIAL_OPENCL_PROGRAM";


    /**
     * Message received form native
     */
    public static final String START_OPENCL_RUNTIME_ACK = "START_OPENCL_RUNTIME_ACK";

    public static String constructStartOpenCLRuntimeMsg(String aoclKernelFileName,String kernelFunctionName,int batchSize,int tupleParallelism,
                                                        String[] inputDataTypes,int[] inShmids,String[] outputDataTypes,int[] outShmids,int shmFlagid){
        StringBuilder builder = new StringBuilder(INITIAL_OPENCL_PROGRAM);
        builder.append("<aoclKernelFile>" + aoclKernelFileName + "<aoclKernelFile>");
        builder.append("<kernelFunctionName>" + kernelFunctionName + "<kernelFunctionName>");
        builder.append("<batchSize>"  + batchSize + "<batchSize>");
        builder.append("<tupleParallelism>" + tupleParallelism + "<tupleParallelism>");
        builder.append("<shmFlagId>" + shmFlagid + "<shmFlagId>");
        builder.append("<inputDataInfo>");
        for(int i = 0 ; i< inputDataTypes.length; i++){
            builder.append("<"+ inputDataTypes[i] + "," + inShmids[i] + ">");
        }
        builder.append("<inputDataInfo>");
        builder.append("<outputDataInfo>");
        for(int i = 0 ; i< outputDataTypes.length; i++){
            builder.append("<"+ outputDataTypes[i] + "," + outShmids[i] + ">");
        }
        builder.append("<outputDataInfo>");
        return builder.toString();
    }

}
