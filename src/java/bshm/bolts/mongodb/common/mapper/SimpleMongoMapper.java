package bshm.bolts.mongodb.common.mapper;

import org.apache.storm.tuple.ITuple;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.bson.Document;

public class SimpleMongoMapper implements MongoMapper {

    private String[] fields;

    @Override
    public Document toDocument(ITuple tuple) {
        Document document = new Document();
        for(String field : fields){
            document.append(field, tuple.getValueByField(field));
        }
        return document;
    }

    public void withFields(String[] fields) {
        this.fields = fields;
    }
}
