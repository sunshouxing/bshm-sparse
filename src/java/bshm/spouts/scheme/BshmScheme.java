package bshm.spouts.scheme;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;

public class BshmScheme implements Scheme {

    @Override
    public List<Object> deserialize(ByteBuffer buffer) {
        Base64.Encoder encoder = Base64.getEncoder();

        ByteBuffer encoded = encoder.encode(buffer);
        String data = new String(encoded.array());

        return Utils.tuple(data);
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("data");
    }

}
