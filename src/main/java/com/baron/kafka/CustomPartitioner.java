package com.baron.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/***
 @package com.baron.kafka
 @author Baron
 @create 2020-09-12-4:47 PM
 */
public class CustomPartitioner  implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> infos = cluster.partitionsForTopic(topic);
        int num = infos.size();

        if (null == keyBytes || !(key instanceof String)) {
            throw new InvalidRecordException("kafka message must have key");
        }
        if (num == 1) {
            return 0;
        }
        if (key.equals("name")) {
            return num - 1;
        }


        return Math.abs(Utils.murmur2(keyBytes)) % (num - 1);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
