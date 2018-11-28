package org.apache.storm.starter;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.accelerate.BaseRichAccBolt;
import org.apache.storm.topology.accelerate.ConstantParameter;
import org.apache.storm.topology.accelerate.DataType;
import org.apache.storm.topology.accelerate.TupleInnerDataType;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2018/11/27.
 */
public class FStormGrep {
    private static final Logger LOG = Logger.getLogger(FStormGrep.class);

    public static final String SPOUT_ID = "spout";
    public static final String SPLIT_ID = "split";
    public static final String FM_ID = "find";
    public static final String CM_ID = "count";

    public static class SentenceGeneratorSpout extends BaseRichSpout {
        static final String[] SENTENCES = new String[]{"the cow jumped over the moon", "an apple a day keeps the doctor away",
                "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature"};

        SpoutOutputCollector _collector;
        long _periodNano;
        long _emitAmount;
        Random _rand;
        long _nextEmitTime;
        long _emitsLeft;

        public SentenceGeneratorSpout(long ratePerSecond) {
            if (ratePerSecond > 0) {
                _periodNano = Math.max(1, 1000000000 / ratePerSecond);
                _emitAmount = Math.max(1, (long) ((ratePerSecond / 1000000000.0) * _periodNano));
            } else {
                _periodNano = Long.MAX_VALUE - 1;
                _emitAmount = 1;
            }
        }

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
            _rand = ThreadLocalRandom.current();
            _nextEmitTime = System.nanoTime();
            _emitsLeft = _emitAmount;
        }

        @Override
        public void nextTuple() {
            if (_emitsLeft <= 0 && _nextEmitTime <= System.nanoTime()) {
                _emitsLeft = _emitAmount;
                _nextEmitTime = _nextEmitTime + _periodNano;
            }

            if (_emitsLeft > 0) {
                String sentence = SENTENCES[_rand.nextInt(SENTENCES.length)];
                _collector.emit(new Values(sentence), sentence);
                _emitsLeft--;
            }
        }


        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
        }
    }

    public static class SplitSentence extends BaseRichBolt {

        public static final String FIELDS = "word";
        public OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            for (String word : input.getString(0).split("\\s+")) {
                _collector.emit(input, new Values(word.toCharArray()));
            }
            _collector.ack(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS));
        }

    }

    public static class FindMatchingWord extends BaseRichAccBolt {
        public static final String FIELDS = "matching";
        private Pattern pattern;
        private Matcher matcher;
        private final String ptnString;
        private OutputCollector _collector;

        public FindMatchingWord(TupleInnerDataType[] inputTupleEleTypes, TupleInnerDataType[] outputTupleEleTypes, ConstantParameter[] constantParameters,
                                int batchSize, String kernelName, String ptnString) {
            super(inputTupleEleTypes, outputTupleEleTypes, constantParameters, batchSize, kernelName);
            this.ptnString = ptnString;
        }

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
            pattern = Pattern.compile(ptnString);
        }

        public List<Object> getInputTupleValues(Tuple tuple) {
            return tuple.getValues();
        }

        @Override
        public void execute(Tuple input) {
            String word = new String((char[])input.getValue(0));
            LOG.debug(String.format("find pattern %s in word %s", ptnString, word));
            matcher = pattern.matcher(word);
            if (matcher.find()) {
                _collector.emit(input, new Values(1));
            } else {
                _collector.emit(input, new Values(0));
            }
            _collector.ack(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS));
        }
    }

    public static class CountMatchingWord extends BaseRichBolt {
        public static final String FIELDS = "count";
        private int count = 0;
        private OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            if (input.getInteger(0).equals(1)) {
                _collector.emit(new Values(count++));
            }
            _collector.ack(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FIELDS));
        }
    }

    private static long _prev_acked = 0;
    private static long _prev_uptime = 0;
    private static double _prev_weightedAvgTotal = 0;

    public static void printMetrics(Nimbus.Client client, String name) throws Exception {
        ClusterSummary summary = client.getClusterInfo();
        String id = null;
        for (TopologySummary ts : summary.get_topologies()) {
            if (name.equals(ts.get_name())) {
                id = ts.get_id();
            }
        }
        if (id == null) {
            throw new Exception("Could not find a topology named " + name);
        }
        TopologyInfo info = client.getTopologyInfo(id);
        int uptime = info.get_uptime_secs();
        long acked = 0;
        long failed = 0;
        double weightedAvgTotal = 0.0;
        for (ExecutorSummary exec : info.get_executors()) {
            if (SPOUT_ID.equals(exec.get_component_id())) {
                SpoutStats stats = exec.get_stats().get_specific().get_spout();
                Map<String, Long> failedMap = stats.get_failed().get(":all-time");
                Map<String, Long> ackedMap = stats.get_acked().get(":all-time");
                Map<String, Double> avgLatMap = stats.get_complete_ms_avg().get(":all-time");
                for (String key : ackedMap.keySet()) {
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
        double avgLatencyThisTime = weightedAvgTotalThisTime / ackedThisTime;
        System.out.println("uptime: " + uptime + "-" + _prev_uptime + " ackedThisTime: " + ackedThisTime + " avgLatency: " + avgLatencyThisTime + " acked/sec: " + (((double) ackedThisTime) / thisTime + " failed: " + failed));
        _prev_uptime = uptime;
        _prev_acked = acked;
        _prev_weightedAvgTotal = weightedAvgTotal;
    }

    public static void kill(Nimbus.Client client, String name) throws Exception {
        KillOptions opts = new KillOptions();
        opts.set_wait_secs(0);
        client.killTopologyWithOpts(name, opts);
    }

    public static final int wordLength = 15;

    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 10) {
            System.out.println("Please input paras: spoutNum splitNum matchNum countNum numAckers numWorkers ratePerSecond batchSize isDebug matchStr");
        } else {
            int spoutNum = Integer.valueOf(args[0]);
            int splitNum = Integer.valueOf(args[1]);
            int matchNum = Integer.valueOf(args[2]);

            int countNum = Integer.valueOf(args[3]);
            int numAckers = Integer.valueOf(args[4]);
            int numWorkers = Integer.valueOf(args[5]);

            int ratePerSecond = Integer.valueOf(args[6]);
            int batchSize = Integer.valueOf(args[7]);
            boolean isDebug = Boolean.valueOf(args[8]);

            String patterStr = args[9];
            Config conf = new Config();

            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout(SPOUT_ID, new SentenceGeneratorSpout(ratePerSecond), spoutNum);
            builder.setBolt(SPLIT_ID, new SplitSentence(), splitNum).shuffleGrouping(SPOUT_ID);
            builder.setAccBolt(FM_ID, new FindMatchingWord(new TupleInnerDataType[]{new TupleInnerDataType(DataType.CHAR, true, wordLength)},
                    new TupleInnerDataType[]{new TupleInnerDataType(DataType.INT, false, 1)},
                    null, batchSize, "findMatchingWord", patterStr), matchNum).localOrShuffleGrouping(SPLIT_ID);
            builder.setBolt(CM_ID, new CountMatchingWord(), countNum).fieldsGrouping(FM_ID, new Fields(FindMatchingWord.FIELDS));

            builder.setTopologyKernelFile("grep");

            conf.setDebug(isDebug);
            conf.setNumAckers(numAckers);
            conf.setNumWorkers(numWorkers);

            String name = "FStormGrep"; //拓扑名称

            StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());


            Map clusterConf = Utils.readStormConfig();
            clusterConf.putAll(Utils.readCommandLineOpts());
            Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();

            Thread.sleep(1000 * 60);
            for (int i = 0; i < 30; i++) {
                Thread.sleep(30 * 1000);
                printMetrics(client, name);
            }
            kill(client, name);
        }
    }
}
