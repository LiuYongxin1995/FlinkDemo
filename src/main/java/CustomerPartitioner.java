import org.apache.flink.api.common.functions.Partitioner;

public class CustomerPartitioner implements Partitioner {


    public int partition(Object o, int i) {
        return Math.abs(o.hashCode() % i);
    }
}
