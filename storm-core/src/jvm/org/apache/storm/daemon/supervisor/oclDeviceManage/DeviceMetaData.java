package org.apache.storm.daemon.supervisor.oclDeviceManage;

/**
 * Created by Administrator on 2017/12/25.
 */
public class DeviceMetaData {
    private static int ocl_fpga_device_num;
    private static int ocl_gpu_device_num;
    private static DeviceMetaData deviceMetaData;
    private DeviceMetaData(){}
    static DeviceMetaData getInstance(){
        if(deviceMetaData == null){
            deviceMetaData = new DeviceMetaData();
        }
        return deviceMetaData;
    }
    public void setOcl_fpga_device_num(int ocl_fpga_device_num) {
        this.ocl_fpga_device_num = ocl_fpga_device_num;
    }

    public void setOcl_gpu_device_num(int ocl_gpu_device_num) {
        this.ocl_gpu_device_num = ocl_gpu_device_num;
    }


    public int getOcl_fpga_device_num() {
        return ocl_fpga_device_num;
    }

    public int getOcl_gpu_device_num() {
        return ocl_gpu_device_num;
    }



}
