package com.company;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Partitioning {

    public static void assignTopicBinPack(
            final Map<String, List<TopicPartition>> assignment,
            final String topic,
            final List<String> consumers,
            final List<TopicPartitionArrivalRate> partitionsArrivalRate) {
        if (consumers.isEmpty()) {
            return;
        }// Track total lag assigned to each consumer (for the current topic)
        final Map<String, Long> consumerTotalArrivalRate = new HashMap<>(consumers.size());
        final Map<String, Integer> consumerTotalPartitions = new HashMap<>(consumers.size());
        final Map<String, Long> consumerRemainingAllowableArrivalRate = new HashMap<>(consumers.size());
        final Map<String, Long> consumerAllowableArrivalRate = new HashMap<>(consumers.size());


        for (String memberId : consumers) {
            consumerTotalArrivalRate.put(memberId, 0L);
            consumerAllowableArrivalRate.put(memberId, 100L);
        }

        // Track total number of partitions assigned to each consumer (for the current topic)
        for (String memberId : consumers) {
            consumerTotalPartitions.put(memberId, 0);
            consumerRemainingAllowableArrivalRate.put(memberId, consumerAllowableArrivalRate.get(memberId));
        }

        // Assign partitions in descending order of lag, then ascending by partition
        //First fit decreasing
        partitionsArrivalRate.sort((p1, p2) -> {
            // If lag is equal, lowest partition id first
            if (p1.getArrivalrate() == p2.getArrivalrate()) {
                return Integer.compare(p1.getPartition(), p2.getPartition());
            }
            // Highest lag first
            return Long.compare(p2.getArrivalrate(), p1.getArrivalrate());
        });
        for (TopicPartitionArrivalRate partition : partitionsArrivalRate) {
            // Assign to the consumer with least number of partitions, then smallest total lag, then smallest id
            // returns the consumer with lowest assigned partitions, if all assigned partitions equal returns the min total lag
            final String memberId = Collections
                    .min(consumerTotalArrivalRate.entrySet(), (c1, c2) -> {
                        // Lowest partition count first
                        final int comparePartitionCount = Integer.compare(consumerTotalPartitions.get(c1.getKey()),
                                consumerTotalPartitions.get(c2.getKey()));
                        if (comparePartitionCount != 0) {
                            return comparePartitionCount;}
                        // If partition count is equal, lowest total lag first
                        final int compareTotalLags = Long.compare(c1.getValue(), c2.getValue());
                        if (compareTotalLags != 0) {
                            return compareTotalLags;}
                        // If total lag is equal, lowest consumer id first
                        return c1.getKey().compareTo(c2.getKey());
                    }).getKey();
            //we currently have the the consumer with the lowest lag
            System.out.println("Assigning the consumer {} with the lowest ArrivalRate {} to the partition with the highest ArrivalRate {} "+
                    memberId + "  " + consumerTotalArrivalRate.get(memberId) + " " + partition.getArrivalrate());

            TopicPartition p =  new TopicPartition(partition.getTopic(), partition.getPartition());


            assignment.get(memberId).add(p);
            consumerTotalArrivalRate.put(memberId, consumerTotalArrivalRate.getOrDefault(memberId, 0L) + partition.getArrivalrate());
            consumerTotalPartitions.put(memberId, consumerTotalPartitions.getOrDefault(memberId, 0) + 1);
            consumerRemainingAllowableArrivalRate.put(memberId, consumerAllowableArrivalRate.get(memberId)
                    - consumerTotalArrivalRate.get(memberId));
            System.out.println("The remaining allowable lag for consumer {} is {} " +
                    memberId + " " + (consumerAllowableArrivalRate.get(memberId) - consumerTotalArrivalRate.get(memberId)));

            System.out.println(
                    "Assigned partition {}-{} to consumer {}.  partition_lag={}, consumer_current_total_lag={} " +
                     " " + partition.getTopic() +
                    " " + partition.getPartition() +
                    " " + memberId +
                    " " + partition.getArrivalrate() +
                   " " + consumerTotalArrivalRate.get(memberId));
        }
    }


    static class TopicPartitionArrivalRate {

        private final String topic;
        private final int partition;
        private  long arrivalrate;

        TopicPartitionArrivalRate(String topic, int partition, long arrivalrate) {
            this.topic = topic;
            this.partition = partition;
            this.arrivalrate = arrivalrate;
        }

        public long getArrivalrate() {
            return arrivalrate;
        }

        public void setArrivalrate(long arrivalrate) {
            this.arrivalrate = arrivalrate;
        }

        String getTopic() {
            return topic;
        }

        int getPartition() {
            return partition;
        }



    }
}
