package bshm.spouts.scheme;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;

public class BshmScheme implements Scheme {
    
    private static Base64.Encoder encoder = Base64.getEncoder();

    @Override
    public List<Object> deserialize(ByteBuffer buffer) {
        byte[] encoded = encoder.encode(buffer.array());
        String data = new String(encoded);

        return Utils.tuple(data);
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("data");
    }

}
