package com.mykafka.partitioner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.springframework.stereotype.Component;

@Component
public class TenantPartitioner implements Partitioner {
    private Map<String, Integer> tenantWeightMap;
    private Map<String, List<Integer>> tenantPartition;

    public void configure(Map<String, ?> configs) {
        // initilize tenant vs weight map here
        tenantWeightMap = getTenantWeights();
        tenantPartition = computePartitionForTenant();
    }

    private Map getTenantWeights() {
        Map m = new HashMap();
        m.put("1001",50);
        m.put("1002",30);
        m.put("1003",30);
        return m;
    }

    private Map computePartitionForTenant(){
        Map m = new HashMap();
        //TODO: populate the tenant vs partition at bootstrap??
        return m;
    }

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        //get the current cluster size.
        int numPartitions = partitions.size();
        int numOfPartitionForTenant = (int) (numPartitions * ((double)tenantWeightMap.get(key)/100.0));

        int whichPartition = 0;
        if(tenantPartition.get(key) == null){
            if (keyBytes != null) {
                int random = Utils.toPositive(Utils.murmur2(keyBytes));
                whichPartition = random % numPartitions;
            }
            else {
                whichPartition = Utils.toPositive(Utils.murmur2(valueBytes)) % (numPartitions - numOfPartitionForTenant) +
                    numOfPartitionForTenant;
            }
            List <Integer> partitionsForTenant = new ArrayList<Integer>();
            partitionsForTenant.add(whichPartition);
            //TODO: find a way to add all applicable paritions for the tenant
            tenantPartition.put((String)key,partitionsForTenant);
        }
        else{
            //TODO: find other partitions also. Right now picking only 0th element
            whichPartition = tenantPartition.get(key).get(0);
        }

        System.out.println("Key = " + (String) key + " Partition = " + whichPartition);

        return whichPartition;
    }

    public void close() {
    }
}

