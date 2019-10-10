import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class FlinkDemo implements Serializable {

    public final static String[] IP = {"10.10.46.101", "10.10.46.102", "10.10.46.103"};

    private static Map<String, String> idWordMap = new HashMap<String, String>();

    public static void main(String[] args) {
        //RestClient client=RestClient.builder(new HttpHost("192.168.11.10",9200)).build();
        FlinkDemo demo = new FlinkDemo();
        demo.wordcount();
    }

    public void wordcount() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.setParallelism(5);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.10.46.101:19092,10.10.46.102:19092");
        props.put("group.id", "test-consumer-group");
        props.put("enable.auto.commit", "true");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        DataStreamSource<String> source = env.addSource(
                new FlinkKafkaConsumer<String>("web_test", new SimpleStringSchema(), props).setStartFromEarliest());

        source.setParallelism(3);

        // source.shuffle();
        //source.partitionCustom(new CustomerPartitioner(),0);

        //env.fromCollection(ListBuffer("spark", "flink"))


        DataStream<Tuple3<String, String, Long>> stream = source.flatMap(new MessageSplitter())
                .keyBy(new KeySelector<Tuple3<String, String, Long>, Tuple2<String, String>>() {
                    public Tuple2<String, String> getKey(Tuple3<String, String, Long> tuple) {
                        return Tuple2.of(tuple.f0, tuple.f1);
                    }
                })                      // 按照name 和 time分组
                .reduce(new SummingReducer());    // 对count累加
//        DataStream<Tuple3<String, String, Long>> stream = source.flatMap(new MessageSplitter()).
//                partitionCustom(new CustomerPartitioner(), 0)
//                .keyBy(0).sum(2);

        stream.print();

        try {
            // stream.writeAsText("data.txt/res"); // 写到文件
            List<HttpHost> httpHosts = new ArrayList<HttpHost>();
            for (String ip : IP) {
                httpHosts.add(new HttpHost(ip, 9200, "http"));
            }
            ElasticsearchSink.Builder<Tuple3<String, String, Long>> esSinkBuilder = new ElasticsearchSink.Builder<Tuple3<String, String, Long>>(
                    httpHosts,
                    new ElasticsearchSinkFunction<Tuple3<String, String, Long>>() {
                        public IndexRequest createIndexRequest(Tuple3<String, String, Long> element) {
                            // String id = UUID.randomUUID().toString();
                            Map<String, String> json = new HashMap<String, String>();
                            json.put("word", element.f0);
                            json.put("ip", element.f1);
                            json.put("count", element.f2.toString());
                            // idWordMap.put(element.f0, id);
                            return Requests.indexRequest()
                                    .index("flink")
                                    .type("user")
                                    //.id(id)
                                    .source(json);
                        }

                        public UpdateRequest updateIndexRequest(Tuple3<String, String, Long> element) {
                            String word = element.f0;
                            long count = element.f2;
                            String id = idWordMap.get(word);
                            UpdateRequest updateRequest = new UpdateRequest();
                            //设置表的index和type,必须设置id才能update
                            try {
                                updateRequest.index("flink").type("user").id(id).
                                        doc(XContentFactory.jsonBuilder().startObject().field("count",
                                                String.valueOf(count)).endObject());
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            return updateRequest;
                        }

                        public void process(Tuple3<String, String, Long> element, RuntimeContext ctx, RequestIndexer indexer) {
//                            if (idWordMap.containsKey(element.f0)) {
//                                indexer.add(updateIndexRequest(element));
//                            } else {
//                                indexer.add(createIndexRequest(element));
//                            }
                            indexer.add(createIndexRequest(element));
                        }
                    }
            );
            esSinkBuilder.setBulkFlushMaxActions(1);
            esSinkBuilder.setBulkFlushMaxSizeMb(500);
            //论缓冲操作的数量或大小如何都要刷新的时间间隔
            esSinkBuilder.setBulkFlushInterval(5000);
            stream.addSink(esSinkBuilder.build());
            env.execute("Kafka sum test");

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    class SummingReducer implements ReduceFunction<Tuple3<String, String, Long>> { // flink 内的聚合函数
        public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1, Tuple3<String, String, Long> value2) {
            return new Tuple3<String, String, Long>(value1.f0, value1.f1, value1.f2 + value2.f2);
        }
    }

    class MessageSplitter implements FlatMapFunction<String, Tuple3<String, String, Long>> { // 将kafka字符串解析到tuple

        public void flatMap(String s, Collector<Tuple3<String, String, Long>> collector) throws Exception {
            if (s != null && !"".equals(s)) {
                String[] arrayString = StringUtils.splitByWholeSeparator(s, "(!@#$%)");
                int length = arrayString.length;
                String ip = arrayString[length - 3];
                String message = arrayString[length - 1];
                String[] wordArray = message.split("[^a-zA-Z]");
                for (String str : wordArray) {
                    if (str != null && str.trim().length() != 0) {
                        collector.collect(new Tuple3<String, String, Long>(str,
                                ip, 1L));
                    }
                }
            }

        }
    }


}