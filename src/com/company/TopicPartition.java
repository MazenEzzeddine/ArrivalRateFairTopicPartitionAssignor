package com.company;

import java.io.Serializable;
import java.util.Objects;

public final class TopicPartition implements Serializable {
    private static final long serialVersionUID = -613627415771699627L;
    private final int partition;
    private final String topic;

    public TopicPartition(String topic, int partition) {
        this.partition = partition;
        this.topic = topic;
    }

    public int partition() {
        return this.partition;
    }

    public String topic() {
        return this.topic;
    }

    @Override
    public int hashCode() {
        int result = partition;
        result = 31 * result + topic.hashCode();
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (this.getClass() != obj.getClass()) {
            return false;
        } else {
            TopicPartition other = (TopicPartition)obj;
            return this.partition == other.partition && Objects.equals(this.topic, other.topic);
        }
    }

    public String toString() {
        return this.topic + "-" + this.partition;
    }
}