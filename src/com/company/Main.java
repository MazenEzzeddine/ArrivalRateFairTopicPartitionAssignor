package com.company;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Main {

    public static void main(String[] args) {

        Map<String,List<TopicPartition>>  ass= new HashMap<>();
        ass.put("c0", new ArrayList<>());
        ass.put("c1", new ArrayList<>());
        ass.put("c2", new ArrayList<>());


        List<String> consumers = new ArrayList<>();

        consumers.add("c0");
        consumers.add("c1");
        consumers.add("c2");

        // write your code here

        System.out.println("Hello There");
        TopicPartition tp0 = new TopicPartition("mazen", 0);
        TopicPartition tp1 = new TopicPartition("mazen", 1);
        TopicPartition tp2 = new TopicPartition("mazen", 2);
        TopicPartition tp3 = new TopicPartition("mazen", 3);
        TopicPartition tp4 = new TopicPartition("mazen", 4);



        List<Partitioning.TopicPartitionArrivalRate> partitionsArrivalRate = new ArrayList<>();

        Partitioning.TopicPartitionArrivalRate tpa0 = new Partitioning.TopicPartitionArrivalRate("mazen", 0, 55);
        Partitioning.TopicPartitionArrivalRate tpa1 = new Partitioning.TopicPartitionArrivalRate("mazen", 1, 55);
        Partitioning.TopicPartitionArrivalRate tpa2 = new Partitioning.TopicPartitionArrivalRate("mazen", 2, 25);
        Partitioning.TopicPartitionArrivalRate tpa3 = new Partitioning.TopicPartitionArrivalRate("mazen", 3, 25);
        Partitioning.TopicPartitionArrivalRate tpa4 = new Partitioning.TopicPartitionArrivalRate("mazen", 4, 25);

        List<TopicPartition> tp = new ArrayList<>();
        tp.add(tp0);
        tp.add(tp1);
        tp.add(tp2);
        tp.add(tp3);
        tp.add(tp4);


        List<Partitioning.TopicPartitionArrivalRate> tpa = new ArrayList<>();
        tpa.add(tpa0);
        tpa.add(tpa1);
        tpa.add(tpa2);
        tpa.add(tpa3);
        tpa.add(tpa4);


        Partitioning.assignTopicBinPack(ass,"mazen", consumers,tpa);


        for( String mi : ass.keySet()) {
            for(TopicPartition tpi : ass.get(mi)){
                System.out.println("Consumer " + mi + " " + tpi.partition());
            }

        }

    }
}
