import net.sf.json.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.*;

public class FlinkToEs {

    public final static String[] IP = {"10.10.46.101", "10.10.46.102", "10.10.46.103"};

    public static Map<String, String> markMap;

    private static RestHighLevelClient client;


    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.10.46.101:19092,10.10.46.102:19092");
        props.put("group.id", "flink_test");
        props.put("enable.auto.commit", "true");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        client = EsUtil.getClient();
        //EsUtil.timeTask(client);


        DataStreamSource<String> source = env.addSource(
                new FlinkKafkaConsumer<String>("window_test", new SimpleStringSchema(), props));
        source.setParallelism(3);

        DataStream<Tuple5<String, String, String, Long, Long>> stream = source
                .flatMap(new FlatMapFunction<String, Tuple5<String, String, String, Long, Long>>() {
                    public void flatMap(String s, Collector<Tuple5<String, String, String, Long, Long>> collector) throws Exception {
                        if (s != null && !"".equals(s)) {
                            String[] arrayString = StringUtils.splitByWholeSeparator(s, "(!@#$%)");
                            int length = arrayString.length;
                            if (length == 4) {
                                String ip = arrayString[0];
                                String filename = arrayString[1];
                                String message = arrayString[2];
                                long time = Long.parseLong(arrayString[3]);
                                long rank=1;
                                collector.collect(new Tuple5<String, String, String, Long, Long>(ip,
                                        filename, message, time, rank));
                            }
                        }
                    }
                })
                .partitionCustom(new CustomerPartitioner(), 2);
        stream.print();
        try {
            List<HttpHost> httpHosts = new ArrayList<HttpHost>();
            for (String ip : IP) {
                httpHosts.add(new HttpHost(ip, 9200, "http"));
            }

            ElasticsearchSink.Builder<Tuple5<String, String, String, Long, Long>> esSinkBuilder = new ElasticsearchSink.Builder<Tuple5<String, String, String, Long, Long>>(
                    httpHosts,
                    new ElasticsearchSinkFunction<Tuple5<String, String, String, Long, Long>>() {
                        public IndexRequest createIndexRequest(Tuple5<String, String, String, Long, Long> element) {
                            Template template = new Template();
                            template.setFilename(element.f1);
                            template.setMessage(element.f2);
                            template.setTime(element.f3);
                            template.setRank(element.f4);
                            return Requests.indexRequest()
                                    .index("flinktoes")
                                    .type("user")
                                    .source(JSONObject.fromObject(template));
//                            if (!EsUtil.checkIndexExist(client, "flinktoes")) {
//                                CreateIndexRequest request = new CreateIndexRequest("flinktoes");
//                                XContentBuilder builder = null;
//                                try {
//                                    builder = XContentFactory.jsonBuilder()
//                                            .startObject()
//                                            .field("number_of_shards", "5")
//                                            .field("max_result_window", "10000000")
//                                            .startObject("analysis")
//                                            .startObject("normalizer")
//                                            .startObject("my_normalizer")
//                                            .field("type", "custom")
//                                            .field("filter", "lowercase")
//                                            .endObject()
//                                            .endObject()
//                                            .endObject()
//                                            .endObject();
//                                } catch (IOException e) {
//                                    e.printStackTrace();
//                                }
//
//                                request.settings(builder);
//                                XContentBuilder mapping2 = null;
//                                try {
//                                    mapping2 = XContentFactory.jsonBuilder()
//                                            .startObject()
//                                            .startObject("properties")
//                                            .startObject("filename").field("type", "keyword").endObject()
//                                            .startObject("message")
//                                            .field("type", "keyword")
//                                            .field("ignore_above", "256")
//                                            .field("normalizer", "my_normalizer")
//                                            .endObject()
//                                            .startObject("rank").field("type", "long").endObject()
//                                            .startObject("time").field("type", "long").endObject()
//                                            .endObject()
//                                            .endObject();
//                                    request.mapping("user", mapping2);
//                                    client.indices().create(request, EsUtil.defaultHeaders);
//                                } catch (IOException e) {
//                                    e.printStackTrace();
//                                } finally {
//                                    if (mapping2 != null) {
//                                        mapping2.close();
//                                    }
//                                }
//                            }
//                            IndexRequest indexRequest = new IndexRequest("flinktoes", "user");
//                            indexRequest.source(JSONObject.fromObject(template), XContentType.JSON);
////                            try {
////                                client.index(indexRequest, EsUtil.defaultHeaders);
////                            } catch (IOException e) {
////                                e.printStackTrace();
////                            }
//                            return indexRequest;
                        }

                        public void process(Tuple5<String, String, String, Long, Long> element, RuntimeContext ctx, RequestIndexer indexer) {
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
}
