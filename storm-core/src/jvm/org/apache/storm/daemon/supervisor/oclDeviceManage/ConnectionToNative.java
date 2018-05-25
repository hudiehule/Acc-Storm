package org.apache.storm.daemon.supervisor.oclDeviceManage;

import java.io.*;
import java.net.Socket;

/**
 * Created by Administrator on 2017/12/23.
 */
public class ConnectionToNative {
     private DeviceManager deviceManager = null;
     private Socket conn = null;
     private BufferedOutputStream outputStream = null;
     private BufferedReader reader = null;

    public ConnectionToNative(DeviceManager deviceManager,int port) throws IOException{
        this.deviceManager = deviceManager;
        conn = new Socket("localhost", port);
        conn.setTcpNoDelay(true);
        this.reader =  new BufferedReader(new InputStreamReader(conn.getInputStream()));
        this.outputStream = new BufferedOutputStream(conn.getOutputStream());
    }

    /**
     * send request to OpenCL Host for FPGA devices number and waiti for the result
     * @throws IOException
     */
    public void requestDeviceNum() throws IOException{
        String msg = "REQUEST_DEVICE_NUM";
        try{
            byte[] b = msg.getBytes();
            outputStream.write(b);
            outputStream.flush();
        }catch(IOException e){
            throw new IOException("send request message error");
        }

        try{
            String recvMsg = reader.readLine(); // readLine是一个阻塞函数，当没有数据读取时，就一直会阻塞在那里，而不是返回null,并且只有遇到'/r','/n'或者“/r/n”才会返回
            if ( recvMsg == null ) {
                throw new IOException("connection lost");
            }
            deviceManager.handleMessages(recvMsg);
        }catch(IOException e){
            throw new IOException("receive message exception");
        }
    }

    /**
     * close the connenction to the OpenCL Host
     * @throws IOException
     */
    public void close() throws IOException{
            this.reader.close();
            this.outputStream.close();
            this.conn.close();
    }
}
