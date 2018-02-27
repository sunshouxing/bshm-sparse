package bshm.spouts.scheme;

import org.apache.storm.kafka.KeyValueScheme;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.nio.ByteBuffer;
import java.util.List;

public class BshmKeyValueScheme extends StringScheme implements KeyValueScheme {

    public List<Object> deserializeKeyAndValue(ByteBuffer key, ByteBuffer value) {
        if ( key == null ) { return deserialize(value); }

        key.position(6);
        String file = StringScheme.deserializeString(key);
        String data = StringScheme.deserializeString(value);

        return Utils.tuple(file, data);
    }

    @Override
    public Fields getOutputFields() {
         return new Fields("file", "data");
    }

}

