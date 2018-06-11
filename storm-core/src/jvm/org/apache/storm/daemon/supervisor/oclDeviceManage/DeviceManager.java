package org.apache.storm.daemon.supervisor.oclDeviceManage;

import org.apache.storm.daemon.supervisor.Supervisor;
import org.apache.storm.generated.LocalAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

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
        try{
            this.connectionToNative = new ConnectionToNative(this,port);
        }catch(IOException e){
            LOG.error(e.getMessage()+" DeviceManager can not connect the native machine!");
            System.exit(1);
        }
        isConnected = true;
        LOG.info("Connecting nativeServer at port %d",port);
        requestMetaData();
    }
    public void requestMetaData(){
        try{
            connectionToNative.requestDeviceNum();
            connectionToNative.close();
        }catch(IOException e){
            LOG.error(e.getMessage());
            System.exit(1);
        }
        isConnected = false;
    }

    /**
     * handle the message form the native server, get the number of FPGA
     * the format of the message is like this "oclDeviceInfo<FPGANumber>2<FPGANumber>"
     * @param msg
     */
    public void handleMessages(String msg){
        if(msg.startsWith("oclDeviceInfo")){
            int startIndex = msg.indexOf("<FPGANumber>")+"<FPGANumber>".length();
            int endIndex = msg.indexOf("<FPGANumber>",startIndex);
            String fpgaDeviceNumStr = msg.substring(startIndex,endIndex);
            deviceMeta.setOcl_fpga_device_num(Integer.parseInt(fpgaDeviceNumStr));
            deviceMeta.setOcl_gpu_device_num(0); // set gpu number to 0
        }
        return;
    }



    public DeviceMetaData getDeviceMetaData(){
        return deviceMeta;
    }
}
