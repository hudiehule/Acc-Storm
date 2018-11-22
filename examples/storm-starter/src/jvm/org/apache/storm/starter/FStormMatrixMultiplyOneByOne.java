package org.apache.storm.starter;

/**
 * Created by Administrator on 2018/11/22.
 */

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.accelerate.BaseRichAccBolt;
import org.apache.storm.topology.accelerate.ConstantParameter;
import org.apache.storm.topology.accelerate.DataType;
import org.apache.storm.topology.accelerate.TupleInnerDataType;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;


/**
 * Created by Administrator on 2018/11/9.
 */
public class FStormMatrixMultiplyOneByOne {
    public static class MatrixGenerator extends BaseRichSpout {
        SpoutOutputCollector _collector;
        float[] matrixA;
        float[] matrixB;
        long _periodNano;
        long _emitAmount;
        Random _rand;
        long _nextEmitTime;
        long _emitsLeft;
        int matrixSize;
        public MatrixGenerator(long ratePerSecond, int matrixN){
            if (ratePerSecond > 0) {
                _periodNano = Math.max(1, 1000000000/ratePerSecond);
                _emitAmount = Math.max(1, (long)((ratePerSecond / 1000000000.0) * _periodNano));
            } else {
                _periodNano = Long.MAX_VALUE - 1;
                _emitAmount = 1;
            }
            this.matrixSize = matrixN * matrixN;
        }
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this._collector = collector;
            this._rand = ThreadLocalRandom.current();
            _nextEmitTime = System.nanoTime();
            _emitsLeft = _emitAmount;
            matrixA = new float[matrixSize];
            matrixB = new float[matrixSize];
        }
        @Override
        public void nextTuple(){
            if (_emitsLeft <= 0 && _nextEmitTime <= System.nanoTime()) {
                _emitsLeft = _emitAmount;
                _nextEmitTime = _nextEmitTime + _periodNano;
            }

            if (_emitsLeft > 0) {
                for(int i = 0; i < matrixSize;i++){
                    matrixA[i] = _rand.nextFloat();
                    matrixB[i] = _rand.nextFloat();
                }
                _collector.emit(new Values(matrixA,matrixB),_rand.nextInt());
                _emitsLeft--;
            }
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("matrixA","matrixB"));
        }
    }

    public static class MatrixMultiply extends BaseRichAccBolt {
        public MatrixMultiply(TupleInnerDataType[] inputTupleEleTypes, TupleInnerDataType[] outputTupleEleTypes, ConstantParameter[] constantParameters, int batchSize, String kernelName, int tupleParallelism){
            super(inputTupleEleTypes,outputTupleEleTypes,constantParameters,batchSize,kernelName,tupleParallelism);
        }
        public List<Object> getInputTupleValues(Tuple tuple){
            return tuple.getValues();
        }

        OutputCollector _collector;
        public void prepare(Map conf,TopologyContext context,OutputCollector collector){
            _collector = collector;
        }
        public void declareOutputFields(OutputFieldsDeclarer declarer){
            declarer.declare(new Fields("matrixC"));
        }
        public void execute(Tuple tuple){
            float[] matrixA = (float[])tuple.getValue(0);
            float[] matrixB = (float[])tuple.getValue(1);
            int matrixN= (int)Math.sqrt(matrixA.length);
            float[] matrixC = new float[matrixA.length];
            for(int i = 0; i < matrixN; i++){
                for(int j = 0; j < matrixN;j++){
                    float sum = 0.0f;
                    for(int k = 0; k < matrixN;k++){
                        sum += matrixA[i * matrixN + k] * matrixB[k * matrixN + j];
                    }
                    matrixC[i * matrixN + j] = sum;
                }
            }
            _collector.emit(tuple,new Values(matrixC));
            _collector.ack(tuple);
        }
        public void cleanup(){

        }

    }

    public static class ResultWriter extends BaseRichBolt {
        private static final Logger LOG = LoggerFactory.getLogger(ResultWriter.class);
        OutputCollector _collector;

        public void prepare(Map conf,TopologyContext context,OutputCollector collector){
            _collector = collector;
        }
        public void declareOutputFields(OutputFieldsDeclarer declarer){
        }
        public void execute(Tuple tuple){
            float[] matrixC = (float[])tuple.getValue(0);
            StringBuilder b = new StringBuilder();
            b.append('[');
            for(int i = 0; i< 10;i++){
                b.append(matrixC[i]);
                if(i == 9) b.append(']');
                else b.append(", ");
            }
            LOG.info("matrixC(first ten elements): " + b.toString());
            _collector.ack(tuple);
        }
        public void cleanup(){

        }
    }

    private static long _prev_acked = 0;
    private static long _prev_uptime = 0;
    private static double _prev_weightedAvgTotal = 0;

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
            if ("matrixGenerator".equals(exec.get_component_id())) {
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
        long ackedThisTime = acked - _prev_acked;
        long thisTime = uptime - _prev_uptime;
        double weightedAvgTotalThisTime = weightedAvgTotal - _prev_weightedAvgTotal;
        double avgLatencyThisTime = weightedAvgTotalThisTime/ackedThisTime;
        System.out.println("uptime: "+uptime + "-" + _prev_uptime +" ackedThisTime: "+ackedThisTime+" avgLatency: "+avgLatencyThisTime+" acked/sec: "+(((double)ackedThisTime)/thisTime+" failed: "+failed));
        _prev_uptime = uptime;
        _prev_acked = acked;
        _prev_weightedAvgTotal = weightedAvgTotal;
    }

    public static void kill(Nimbus.Client client, String name) throws Exception {
        KillOptions opts = new KillOptions();
        opts.set_wait_secs(0);
        client.killTopologyWithOpts(name, opts);
    }

    public static void main(String[] args) throws Exception{
        if(args == null ||args.length <9){
            System.out.println("Please input paras: spoutNum bolt1Num bolt2Num numAckers numWorkers ratePerSecond matrixN batchSize isDebug");
        }else{
            int spoutNum = Integer.valueOf(args[0]);
            int bolt1Num = Integer.valueOf(args[1]);
            int bolt2Num = Integer.valueOf(args[2]);

            int numAckers = Integer.valueOf(args[3]);
            int numWorkers = Integer.valueOf(args[4]);

            int ratePerSecond = Integer.valueOf(args[5]);
            int matrixN = Integer.valueOf(args[6]);
            //    String filePath = args[7];
            int batchSize = Integer.valueOf(args[7]);
            boolean isDebug = Boolean.valueOf(args[8]);

            Config conf = new Config();

            TopologyBuilder builder = new TopologyBuilder();
            int matrixSize = matrixN * matrixN;
            builder.setSpout("matrixGenerator",new MatrixGenerator(ratePerSecond,matrixN),spoutNum);
            builder.setAccBolt("matrixMultiply",new MatrixMultiply(
                    // input data type
                    new TupleInnerDataType[]{new TupleInnerDataType(DataType.FLOAT,true,matrixSize),
                            new TupleInnerDataType(DataType.FLOAT,true,matrixSize)},
                    // output data type
                    new TupleInnerDataType[]{new TupleInnerDataType(DataType.FLOAT,true,matrixSize)},
                    new ConstantParameter[]{new ConstantParameter(DataType.INT,matrixN)},
                    batchSize,"matrixMult",matrixSize),bolt1Num).shuffleGrouping("matrixGenerator");
            builder.setBolt("resultWriter",new ResultWriter(),bolt2Num)
                    .shuffleGrouping("matrixMultiply");
            builder.setTopologyKernelFile("matrix_mult_hudie");
            conf.setNumWorkers(numWorkers);
            conf.setNumAckers(numAckers);
            conf.setDebug(isDebug);

            String name = "MatrixMultiplyOnStorm"; //拓扑名称

            StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());


            Map clusterConf = Utils.readStormConfig();
            clusterConf.putAll(Utils.readCommandLineOpts());
            Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();

            Thread.sleep(1000 * 30);
            for (int i = 0; i < 30; i++) {
                Thread.sleep(30 * 1000);
                printMetrics(client, name);
            }
            kill(client, name);

        }

    }
}
