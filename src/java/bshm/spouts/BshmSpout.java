package bshm.spouts;


import bshm.spouts.scheme.BshmKeyValueScheme;
import org.apache.storm.kafka.*;

public class BshmSpout extends KafkaSpout {

    private static SpoutConfig defaultConfig() {
        ZkHosts hosts = new ZkHosts("confluent_zookeeper_1:2888");
        SpoutConfig config = new SpoutConfig(hosts, "bshm", "/kafkaSpout", "bshm_reader");
        config.scheme = new KeyValueSchemeAsMultiScheme(new BshmKeyValueScheme());
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

