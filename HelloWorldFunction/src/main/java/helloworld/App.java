package helloworld;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

/**
 * Handler for requests to Lambda function.
 */
public class App implements RequestHandler<Document, String> {

    private DynamoDB dynamoDB;
    private String tableName = "DHT11";
    private String region = "ap-northeast-2";

    public String handleRequest(final Document input, final Context context) {
        initDynamoDbClient();

        return persistData(input);
    }
    private String persistData(Document document) throws ConditionalCheckFailedException {

        SimpleDateFormat sdf = new SimpleDateFormat ( "yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"));
        String timeString = sdf.format(new java.util.Date (document.timestamp*1000));

        if (document.current.state.reported.temperature.equals(document.previous.state.reported.temperature) &&
                document.current.state.reported.humidity.equals(document.previous.state.reported.humidity)) {
            return null;
        }

        return dynamoDB.getTable(tableName)
                .putItem(new PutItemSpec().withItem(new Item().withPrimaryKey("id", document.device)
                        .withLong("time", document.timestamp)
                        .withLong("temperature", document.current.state.reported.temperature)
                        .withLong("humidity", document.current.state.reported.humidity)
                        .withString("timestamp",timeString)))
                .toString();
    }

    private void initDynamoDbClient() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(region).build();

        this.dynamoDB = new DynamoDB(client);
    }
}

class Document {
    public Thing previous;
    public Thing current;
    public long timestamp;
    public String device;       // AWS IoT에 등록된 사물 이름
}

class Thing {
    public State state = new State();
    public long timestamp;
    public String clientToken;

    public class State {
        public Tag reported = new Tag();
        public Tag desired = new Tag();

        public class Tag {
            public Long temperature;
            public Long humidity;
        }
    }
}