package org.apache.storm.daemon.supervisor.oclDeviceManage;

import java.io.*;
import java.net.Socket;

/**
 * Created by Administrator on 2017/12/23.
 */
public class ConnectionToNative {
    //    private static final int BUFFER_SIZE = 1024;
     DeviceManager deviceManager = null;
     Listener listener= null;
     Sender sender = null;

    public ConnectionToNative(DeviceManager deviceManager,Socket conn){
        this.deviceManager = deviceManager;
        this.listener = new Listener(conn);
        this.sender = new Sender(conn);
        this.listener.start();
    }
    class Listener extends Thread{
        Socket conn = null;
        boolean listening = true;
        BufferedReader reader = null;

        Listener(Socket conn){
            this.conn = conn;
            this.setName("NativeListener");
            try {
                this.reader =  new BufferedReader(new InputStreamReader(conn.getInputStream()));
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        @Override
        public void run(){
          //  InputStream instream = null;
            try {
                while ( listening ) {
                    // readLine是一个阻塞函数，当没有数据读取时，就一直会阻塞在那里，而不是返回null,并且只有遇到'/r','/n'或者“/r/n”才会返回
                    String recvMsg = reader.readLine();
                    if ( recvMsg == null ) {
                        // Connection lost
                        return;
                    }
                    deviceManager.handleMessages(recvMsg);
                }
            }/* catch ( StreamCorruptedException sce) {
                // skip over the bad bytes
                try {
                    if ( instream != null )
                        instream.skip(instream.available());
                } catch ( Exception e1 ) {
                    listening = false;
                }
            }*/ catch ( Exception e ) {
                e.printStackTrace();
                listening = false;
            }
        }
    }
    class Sender {
        Socket conn = null;
        BufferedOutputStream os = null;

        Sender(Socket conn){
            try {
                this.conn = conn;
                this.conn.setTcpNoDelay(true);
                this.os = new BufferedOutputStream( conn.getOutputStream());
            }
            catch ( Exception e ) {
                e.printStackTrace();
            }
        }
        private void serializeAndSendString(String msg){
            try{
                byte[] b = msg.getBytes();
                os.write(b);
                os.flush();
            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }
    public void requestDeviceNum() {
        sender.serializeAndSendString("REQUEST_DEVICE_NUM");
    }
}
