import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Properties;

public class flinkWaterMark {

    public static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
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
        try {
            DataStream<Tuple3<String, Long, Long>> orderingWindowStreamsByKey =
                    source.flatMap(new MessageSplitHandle())
                            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGeneratorMark())
                            .keyBy(0)
                            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                            .apply(new WindowFunctionTest());
            orderingWindowStreamsByKey.print();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

class BoundedOutOfOrdernessGeneratorMark implements AssignerWithPeriodicWatermarks<Tuple3<String, Long, Long>> {

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

//WindowFunction全量处理，实时性比较差
class WindowFunctionTest implements WindowFunction<Tuple3<String, Long, Long>, Tuple3<String, Long, Long>, Tuple, TimeWindow> {

    public void apply(Tuple s, TimeWindow timeWindow, Iterable<Tuple3<String, Long, Long>> iterable,
                      Collector<Tuple3<String, Long, Long>> collector) throws Exception {
        Iterator<Tuple3<String, Long, Long>> iterator = iterable.iterator();
        long windowEnd = timeWindow.getEnd();
        long windowStart = timeWindow.getStart();
        long sum = 0;
        while (iterator.hasNext()) {
            Tuple3<String, Long, Long> next = iterator.next();
            sum += next.f2;
        }
        collector.collect(new Tuple3<String, Long, Long>(String.valueOf(sum),
                windowStart, windowEnd));
    }
}

//来一条处理一条，增量处理，实时性强
class TupleReducer implements ReduceFunction<Tuple3<String, Long, Long>> { // flink 内的聚合函数
    public Tuple3<String, Long, Long> reduce(Tuple3<String, Long, Long> value1, Tuple3<String, Long, Long> value2) {
        return new Tuple3<String, Long, Long>(value1.f0, value1.f1, value1.f2 + value2.f2);
    }
}

class MessageSplitHandle implements FlatMapFunction<String, Tuple3<String, Long, Long>> { // 将kafka字符串解析到tuple

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



