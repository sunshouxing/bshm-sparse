package bshm.spouts;

import bshm.spouts.scheme.BshmScheme;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;

public class BshmSpout extends KafkaSpout {

    private static SpoutConfig defaultConfig() {
        ZkHosts hosts = new ZkHosts("confluent_zookeeper_1:2888");
        SpoutConfig config = new SpoutConfig(hosts, "bshm", "/kafkaSpout", "bshm_reader");
        config.scheme = new SchemeAsMultiScheme(new BshmScheme());
        config.ignoreZkOffsets = true;

        return config;
    }

    public BshmSpout(SpoutConfig config) {
        super(config);
    }

    public BshmSpout() {
        this(BshmSpout.defaultConfig());
    }
}

