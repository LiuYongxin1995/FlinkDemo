import java.util.Timer;
import java.util.TimerTask;

public class test {


    public static void main(String[] args) {
//        String str = " WARN org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy: Failed to place enough replicas, still in need of 1 to reach 2 (unavailableStorages=[DISK, ARCHIVE], storagePolicy=BlockStoragePolicy{HOT:7, storageTypes=[DISK], creationFallbacks=[], replicationFallbacks=[ARCHIVE]}, newBlock=false) All required storage types are unavailable: unavailableStorages=[DISK, ARCHIVE], storagePolicy=BlockStoragePolicy{HOT:7, storageTypes=[DISK], creationFallbacks=[], replicationFallbacks=[ARCHIVE]}";
//        String[] array = str.split("[^a-zA-Z]");
//        //String []array=org.apache.commons.lang.StringUtils.split(str,"[^a-zA-Z]\\s+");
//        for (int i = 0; i < array.length; i++) {
//            System.out.println("第" + i + "个=" + array[i]);
//        }
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            public void run() {
                System.out.println(123);

            }
        }, 0, 1000 * 120);

        System.out.println(567);
        System.out.println(892);


    }
}
