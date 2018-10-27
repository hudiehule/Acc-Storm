package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
/**
 * Created by Administrator on 2018/10/26.
 */
public class VectorMultOnStorm {

    public static class VectorGenerator extends BaseRichSpout{
        SpoutOutputCollector _collector;
        float[] vectorA;
        float[] vectorB;
        long _periodNano;
        long _emitAmount;
        Random _rand;
        long _nextEmitTime;
        long _emitsLeft;
        int vectorSize;
        public VectorGenerator(long ratePerSecond, int vectorSize){
            if (ratePerSecond > 0) {
                _periodNano = Math.max(1, 1000000000/ratePerSecond);
                _emitAmount = Math.max(1, (long)((ratePerSecond / 1000000000.0) * _periodNano));
            } else {
                _periodNano = Long.MAX_VALUE - 1;
                _emitAmount = 1;
            }
            vectorA = new float[vectorSize];
            vectorB = new float[vectorSize];
            this.vectorSize = vectorSize;
        }
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this._collector = collector;
            this._rand = ThreadLocalRandom.current();
            _nextEmitTime = System.nanoTime();
            _emitsLeft = _emitAmount;
            for(int i = 0; i < vectorSize;i++){
                vectorA[i] = _rand.nextFloat();
                vectorB[i] = _rand.nextFloat();
            }
        }
        @Override
        public void nextTuple(){
            if (_emitsLeft <= 0 && _nextEmitTime <= System.nanoTime()) {
                _emitsLeft = _emitAmount;
                _nextEmitTime = _nextEmitTime + _periodNano;
            }

            if (_emitsLeft > 0) {
                _collector.emit(new Values(vectorA,vectorB),_rand.nextInt());
                _emitsLeft--;
            }
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("vectorA","vectorB"));
        }
    }

    public static class VectorMultiply extends BaseRichBolt {
        OutputCollector _collector;
        public void prepare(Map conf,TopologyContext context,OutputCollector collector){
            _collector = collector;
        }
        public void declareOutputFields(OutputFieldsDeclarer declarer){
             declarer.declare(new Fields("vectorC"));
        }
        public void execute(Tuple tuple){
            float[] vectorA = (float[])tuple.getValue(0);
            float[] vectorB = ( float[])tuple.getValue(0);
            int vectorSize = vectorA.length;
            System.out.println("the vector size: " + vectorSize);
            for(int i = 0; i< vectorSize;i++){
                System.out.print(vectorA[i] + " ");
            }
            float[] vectorC = new float[vectorSize];
            for(int i = 0; i < vectorSize; i++){
                vectorC[i] = vectorA[i] * vectorB[i];
            }
            _collector.emit(tuple,new Values(vectorC));
            _collector.ack(tuple);
        }
        public void cleanup(){

        }
    }

    public static class ResultWriter extends BaseRichBolt{
        OutputCollector _collector;
        String filePath;
        FileOutputStream fos = null;
        DataOutputStream dos = null;
        public ResultWriter(String filePath){
            this.filePath = filePath;
        }
        public void prepare(Map conf,TopologyContext context,OutputCollector collector){
            _collector = collector;
            try{
                File file = new File(filePath);
                if(!file.exists()){
                    file.createNewFile();
                }
                fos = new FileOutputStream(file);
                dos = new DataOutputStream(fos);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        public void declareOutputFields(OutputFieldsDeclarer declarer){
        }
        public void execute(Tuple tuple){
            float[] vectorC = (float[])tuple.getValue(0);
            try{
                for(int i = 0; i < vectorC.length;i++){
                    dos.writeFloat(vectorC[i]);
                    dos.writeChar(32); // 空格
                }
                dos.writeChar(13); // 换行
            }catch (Exception e){
                e.printStackTrace();
            }
            _collector.ack(tuple);
        }
        public void cleanup(){
            try{
                dos.close();
                fos.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public static void printMetrics(Nimbus.Client client, String name) throws Exception {
        ClusterSummary summary = client.getClusterInfo();
        String id = null;
        for (TopologySummary ts: summary.get_topologies()) {
            if (name.equals(ts.get_name())) {
                id = ts.get_id();
            }
        }
        if (id == null) {
            throw new Exception("Could not find a topology named "+name);
        }
        TopologyInfo info = client.getTopologyInfo(id);
        int uptime = info.get_uptime_secs();
        long acked = 0;
        long failed = 0;
        double weightedAvgTotal = 0.0;
        for (ExecutorSummary exec: info.get_executors()) {
            if ("vectorGenerator".equals(exec.get_component_id())) {
                SpoutStats stats = exec.get_stats().get_specific().get_spout();
                Map<String, Long> failedMap = stats.get_failed().get(":all-time");
                Map<String, Long> ackedMap = stats.get_acked().get(":all-time");
                Map<String, Double> avgLatMap = stats.get_complete_ms_avg().get(":all-time");
                for (String key: ackedMap.keySet()) {
                    if (failedMap != null) {
                        Long tmp = failedMap.get(key);
                        if (tmp != null) {
                            failed += tmp;
                        }
                    }
                    long ackVal = ackedMap.get(key);
                    double latVal = avgLatMap.get(key) * ackVal;
                    acked += ackVal;
                    weightedAvgTotal += latVal;
                }
            }
        }
        double avgLatency = weightedAvgTotal/acked;
        System.out.println("uptime: "+uptime+" acked: "+acked+" avgLatency: "+avgLatency+" acked/sec: "+(((double)acked)/uptime+" failed: "+failed));
    }

    public static void kill(Nimbus.Client client, String name) throws Exception {
        KillOptions opts = new KillOptions();
        opts.set_wait_secs(0);
        client.killTopologyWithOpts(name, opts);
    }

    public static void main(String[] args) throws Exception{
        if(args == null ||args.length <9){
            System.out.println("Please input paras: spoutNum bolt1Num bolt2Num numAckers numWorkers ratePerSecond vectorSize resultFilePath isDebug");
        }else{
            int spoutNum = Integer.valueOf(args[0]);
            int bolt1Num = Integer.valueOf(args[1]);
            int bolt2Num = Integer.valueOf(args[2]);

            int numAckers = Integer.valueOf(args[3]);
            int numWorkers = Integer.valueOf(args[4]);

            int ratePerSecond = Integer.valueOf(args[5]);
            int matrixSize = Integer.valueOf(args[6]);
            String filePath = args[7];
            boolean isDebug = Boolean.valueOf(args[8]);

            Config conf = new Config();

            TopologyBuilder builder = new TopologyBuilder();


            builder.setSpout("vectorGenerator",new VectorGenerator(ratePerSecond,matrixSize),spoutNum);
            builder.setBolt("vectorMultiply",new VectorMultiply(),bolt1Num).shuffleGrouping("vectorGenerator");
            builder.setBolt("resultWriter",new ResultWriter(filePath),bolt2Num).shuffleGrouping("vectorMultiply");

            conf.setNumWorkers(numWorkers);
            conf.setNumAckers(numAckers);
            conf.setDebug(isDebug);

            String name = "VectorMultiplyOnStorm"; //拓扑名称

            StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());


            Map clusterConf = Utils.readStormConfig();
            clusterConf.putAll(Utils.readCommandLineOpts());
            Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();

            for (int i = 0; i < 20; i++) {
                Thread.sleep(30 * 1000);
                printMetrics(client, name);
            }
            kill(client, name);

        }

    }
}
