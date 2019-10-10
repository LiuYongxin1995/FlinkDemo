import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkWarnByThreshold {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

        DataStream<Tuple3<String, Long, Long>> WarringMessage =
                source.flatMap(new MessageSplit())
                        .partitionCustom(new CustomerPartitioner(), 0)
                        .keyBy(0)
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                        .sum(2);
        WarringMessage.print();
        WarringMessage.addSink(new MysqlThresholdSink());
        try {
            env.execute("data to mysql start");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}


