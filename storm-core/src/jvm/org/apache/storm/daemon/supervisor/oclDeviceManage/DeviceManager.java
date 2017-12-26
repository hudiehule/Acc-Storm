package org.apache.storm.daemon.supervisor.oclDeviceManage;

import org.apache.storm.daemon.supervisor.Supervisor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;

/**
 * Created by Administrator on 2017/12/23.
 */
public class DeviceManager {
    private static final Logger LOG = LoggerFactory.getLogger(DeviceManager.class);
    private final String supervisorId;
    private final Supervisor supervisor;
    private ConnectionToNative connectionToNative;
    public boolean isConnected;
    private DeviceMetaData deviceMeta = DeviceMetaData.getInstance();
    public DeviceManager(Supervisor supervisor,int port){
        this.supervisorId = supervisor.getId();
        this.supervisor = supervisor;
        try {
            Socket conn = new Socket("localhost", port);
            conn.setTcpNoDelay(true);
            this.connectionToNative = new ConnectionToNative(this,conn);
            isConnected = true;
            LOG.info("Connecting nativeServer at port %d",port);
        }catch(Exception e){
            isConnected = false;
            LOG.error("DeviceManager can not connect the native machine!");
        }
        requestMetaData();
    }
    public void requestMetaData(){
         connectionToNative.requestDeviceNum();
    }

    public void handleMessages(String msg){
        if(msg.indexOf("OclDeviceInfo")!= -1){
            String[] infos = msg.split("/");
            String fpgaInfo = infos[1];
            String gpuInfo = infos[2];
            int index = fpgaInfo.indexOf("_");
            String fpgaDeviceNumStr = fpgaInfo.substring(index+1);
            deviceMeta.setOcl_fpga_device_num(Integer.parseInt(fpgaDeviceNumStr));
            index = gpuInfo.indexOf("_");
            String gpuDeviceNumStr = gpuInfo.substring(index+1);
            deviceMeta.setOcl_gpu_device_num(Integer.parseInt(gpuDeviceNumStr));
        }
        return;
    }



    public DeviceMetaData getDeviceMetaData(){
        return deviceMeta;
    }
}
