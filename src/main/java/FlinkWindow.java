import net.sf.json.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FlinkWindow {

    public final static String[] IP = {"10.10.46.101", "10.10.46.102", "10.10.46.103"};

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.10.46.101:19092,10.10.46.102:19092");
        props.put("group.id", "flink_test");
        props.put("enable.auto.commit", "true");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        DataStreamSource<String> source = env.addSource(
                new FlinkKafkaConsumer<String>("window_test", new SimpleStringSchema(), props));
        source.setParallelism(3);

        DataStream<Tuple3<String, Long, Long>> orderingWindowStreamsByKey =
                source.flatMap(new MessageSplit())
                        .partitionCustom(new CustomerPartitioner(), 0)
                        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator())
                        .keyBy(0)
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                        .sum(2);
        orderingWindowStreamsByKey.print();
        try {
            List<HttpHost> httpHosts = new ArrayList<HttpHost>();
            for (String ip : IP) {
                httpHosts.add(new HttpHost(ip, 9200, "http"));
            }
            ElasticsearchSink.Builder<Tuple3<String, Long, Long>> esSinkBuilder = new ElasticsearchSink.Builder<Tuple3<String, Long, Long>>(
                    httpHosts,
                    new ElasticsearchSinkFunction<Tuple3<String, Long, Long>>() {
                        public IndexRequest createIndexRequest(Tuple3<String, Long, Long> element) {
                            WindowCount windowCount = new WindowCount();
                            windowCount.setWord(element.f0);
                            windowCount.setCount(element.f1.toString());
                            windowCount.setTime(element.f2);
                            return Requests.indexRequest()
                                    .index("windowcount")
                                    .type("user")
                                    .source(JSONObject.fromObject(windowCount));
                        }

                        public void process(Tuple3<String, Long, Long> element, RuntimeContext ctx, RequestIndexer indexer) {
                            indexer.add(createIndexRequest(element));
                        }
                    }
            );
            esSinkBuilder.setBulkFlushMaxActions(1);
            esSinkBuilder.setBulkFlushMaxSizeMb(500);
            //论缓冲操作的数量或大小如何都要刷新的时间间隔
            esSinkBuilder.setBulkFlushInterval(5000);
            orderingWindowStreamsByKey.addSink(esSinkBuilder.build());
            env.execute("window sum test");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class MessageSplit implements FlatMapFunction<String, Tuple3<String, Long, Long>> { // 将kafka字符串解析到tuple

    public void flatMap(String s, Collector<Tuple3<String, Long, Long>> collector) throws Exception {
        if (s != null && !"".equals(s)) {
            String[] arrayString = StringUtils.splitByWholeSeparator(s, "(!@#$%)");
            int length = arrayString.length;
            if (length == 4) {
                String message = arrayString[2];
                String time = arrayString[3];
                String[] wordArray = message.split("[^a-zA-Z]");
                for (String str : wordArray) {
                    if (str != null && str.trim().length() != 0) {
                        collector.collect(new Tuple3<String, Long, Long>(str, Long.parseLong(time)
                                , 1L));
                    }
                }
            }
        }
    }
}

class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<Tuple3<String, Long, Long>> {

    long maxOutOfOrderness = 3500L;
    long currentMaxTimestamp;

    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    public long extractTimestamp(Tuple3<String, Long, Long> stringStringLongTuple3, long l) {
        long timestamp = stringStringLongTuple3.f2;
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }
}



