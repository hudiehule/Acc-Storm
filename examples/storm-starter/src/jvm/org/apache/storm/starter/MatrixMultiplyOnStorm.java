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
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Administrator on 2018/10/26.
 */
public class MatrixMultiplyOnStorm {
    public static class MatrixGenerator extends BaseRichSpout{
        SpoutOutputCollector _collector;
        float[][] matrixA;
        float[][] matrixB;
        long _periodNano;
        long _emitAmount;
        Random _rand;
        long _nextEmitTime;
        long _emitsLeft;
        int matrixN;
        public MatrixGenerator(long ratePerSecond, int matrixN){
            if (ratePerSecond > 0) {
                _periodNano = Math.max(1, 1000000000/ratePerSecond);
                _emitAmount = Math.max(1, (long)((ratePerSecond / 1000000000.0) * _periodNano));
            } else {
                _periodNano = Long.MAX_VALUE - 1;
                _emitAmount = 1;
            }
            matrixA = new float[matrixN][matrixN];
            matrixB = new float[matrixN][matrixN];
            this.matrixN = matrixN;
        }
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
             this._collector = collector;
             this._rand = ThreadLocalRandom.current();
            _nextEmitTime = System.nanoTime();
            _emitsLeft = _emitAmount;
             for(int i = 0; i < matrixN;i++){
                 for(int j = 0; j< matrixN;j++){
                     matrixA[i][j] = _rand.nextFloat();
                     matrixB[i][j] = _rand.nextFloat();
                 }
             }
        }
        @Override
        public void nextTuple(){
            if (_emitsLeft <= 0 && _nextEmitTime <= System.nanoTime()) {
                _emitsLeft = _emitAmount;
                _nextEmitTime = _nextEmitTime + _periodNano;
            }

            if (_emitsLeft > 0) {
                _collector.emit(new Values(matrixA,matrixB),_rand.nextInt());
                _emitsLeft--;
            }
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("matrixA","matrixB"));
        }
    }

    public static class MatrixMultiply extends BaseRichBolt {
        OutputCollector _collector;
        public void prepare(Map conf,TopologyContext context,OutputCollector collector){
            _collector = collector;
        }
        public void declareOutputFields(OutputFieldsDeclarer declarer){
            declarer.declare(new Fields("matrixC"));
        }
        public void execute(Tuple tuple){
            float[][] matrixA = (float[][])tuple.getValue(0);
            float[][] matrixB = (float[][])tuple.getValue(1);
            int matrixN = matrixA.length;
            float[][] matrixC = new float[matrixN][matrixN];
            for(int i = 0; i < matrixN; i++){
                for(int j = 0; i < matrixN;i++){
                    int sum = 0;
                    for(int k = 0; k < matrixN;k++){
                        sum += matrixA[i][k] * matrixB[k][j];
                    }
                    matrixC[i][j] = sum;
                }
            }
            _collector.emit(tuple,new Values(matrixC));
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
        public void prepare(Map conf,TopologyContext context,OutputCollector collector){
            _collector = collector;
        }
        public void declareOutputFields(OutputFieldsDeclarer declarer){
        }
        public void execute(Tuple tuple){
            float[][] matrixC = (float[][])tuple.getValue(0);
            try{
                for(int i = 0; i < matrixC.length;i++){
                    for(int j = 0; j < matrixC.length;i++)
                    {
                        dos.writeFloat(matrixC[i][j]);
                        dos.writeChar(32); // 空格
                    }
                    dos.writeChar(13); // 换行
                }
            }catch (Exception e){
                e.printStackTrace();
            }
            _collector.ack(tuple);
        }
        public void cleanup(){

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
            if ("spout".equals(exec.get_component_id())) {
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
            System.out.println("Please input paras: spoutNum bolt1Num bolt2Num numAckers numWorkers sleepTime batchSize isDebug");
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


            builder.setSpout("matrixGenerator",new MatrixGenerator(ratePerSecond,matrixSize),spoutNum);
            builder.setBolt("matrixMultiply",new MatrixMultiply(),bolt1Num).shuffleGrouping("matrixGenerator");
            builder.setBolt("resultWriter",new ResultWriter(filePath),bolt2Num).shuffleGrouping("matrixMultiply");

            conf.setNumWorkers(numWorkers);
            conf.setNumAckers(numAckers);
            conf.setDebug(isDebug);

            String name = "MatrixMultiplyOnStorm"; //拓扑名称

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
