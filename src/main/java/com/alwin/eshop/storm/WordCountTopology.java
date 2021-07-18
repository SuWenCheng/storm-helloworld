package com.alwin.eshop.storm;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
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
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@Slf4j
public class WordCountTopology {

    /**
     * spout 负责从数据源获取数据
     */
    public static class RandomSentenceSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;
        private Random random;

        /**
         * open方法 对spout进行初始化
         */
        public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
            this.random = new Random();
        }

        /**
         * 负责spout的task不断无限循环调用此方法获取数据
         */
        public void nextTuple() {
            Utils.sleep(100);
            String[] sentences = new String[]{"whenever you find yourself on the side of the majority", "god helps those who help themselves",
                "beauty without virtue is a rose without fragrance", "a journey of a thousand miles begins with a single step"};
            String sentence = sentences[random.nextInt(sentences.length)];
            log.error("【发射句子】sentence=" + sentence);
            // value, 可认为就是一个tuple
            collector.emit(new Values(sentence));
        }

        /**
         * 定义发射出去的每个tuple中的每个field的名称
         */
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
        }
    }

    /**
     * 每个bolt代码，同样是发送到worker某个executor的task里去运行
     */
    public static class SplitSentence extends BaseRichBolt {

        private OutputCollector collector;

        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        /**
         * 每次接收到一条数据后，就会交给这个execute方法执行
         */
        public void execute(Tuple tuple) {
            String sentence = tuple.getStringByField("sentence");
            String[] words = sentence.split(" ");
            for (String word : words) {
                collector.emit(new Values(word));
            }
        }

        /**
         * 定义发送出去的tuple，每个field的名称
         */
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static class WordCount extends BaseRichBolt {

        private OutputCollector collector;
        private final Map<String, Long> wordCounts = new HashMap<>();

        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        public void execute(Tuple tuple) {
            String word = tuple.getStringByField("word");

            Long count = wordCounts.getOrDefault(word, 0L);
            count ++;

            wordCounts.put(word, count);

            log.error("【单词计数】{} 出现的次数是: {}", word, count);
            collector.emit(new Values(word, count));

        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    /**
     * main方法中，会将spout和bolts组合起来，构建成一个拓扑
     */
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        // 第一个参数是给spout设置名字
        // 第二个参数是创建一个spout对象
        // 第三个参数为spout的executor个数
        builder.setSpout("RandomSentence", new RandomSentenceSpout(), 2);

        // shuffleGrouping表示此bolt对上一层的spout进行shuffle分组
        builder.setBolt("SplitSentence", new SplitSentence(), 5)
                .setNumTasks(10)
                .shuffleGrouping("RandomSentence");

        builder.setBolt("WordCount", new WordCount(), 10)
                .setCPULoad(20)
                .fieldsGrouping("SplitSentence", new Fields("word"));

        Config config = new Config();

        // 说明是在命令行执行，即打算提交到storm集群
        if (args != null && args.length > 0) {
            config.setNumWorkers(3);
            try {
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            // 说明在本地运行
            config.setMaxTaskParallelism(20);

            LocalCluster cluster = null;
            try {
                cluster = new LocalCluster();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                assert cluster != null;
                cluster.submitTopology("WordCountTopology", config, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }

            Utils.sleep(20000);
            cluster.shutdown();
        }
    }

}
