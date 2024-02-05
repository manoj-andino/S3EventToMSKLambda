package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import utils.MapperUtil;
import xml.Inventory;

import java.util.List;
import java.util.Properties;
import java.util.Set;

public class InventoryUpdate implements RequestHandler<S3Event, String> {
    static String bootstrapServers = System.getenv("MSK_BOOTSTRAP_SERVERS");
    static String inventoryUpdateTopic = System.getenv("INVENTORY_UPDATE_TOPIC");
    private KafkaProducer<String, String> producer;
    @Override
    public String handleRequest(S3Event s3Event, Context context) {
        LambdaLogger logger = context.getLogger();
        try {
            List<S3EventNotification.S3EventNotificationRecord> s3EventRecords = s3Event.getRecords();
            S3EventNotification.S3EventNotificationRecord s3EventNotificationRecord = s3EventRecords.getFirst();
            String bucketName = s3EventNotificationRecord.getS3().getBucket().getName();
            String key = s3EventNotificationRecord.getS3().getObject().getKey();

            AmazonS3 amazonS3Client = AmazonS3ClientBuilder.defaultClient();
            String objectAsString = amazonS3Client.getObjectAsString(bucketName, key);

            Inventory inventory = MapperUtil.mapXmlToInventoryObject(objectAsString);
            String payload = MapperUtil.writeAsString(inventory);
            logger.log("Payload " + payload, LogLevel.DEBUG);

            AdminClient admin = AdminClient.create(getKafkaProperties());
            ListTopicsResult listTopics = admin.listTopics();
            Set<String> names = listTopics.names().get();
            logger.log("Kafka topics: " + names, LogLevel.DEBUG);
            if (!names.contains(inventoryUpdateTopic)) {
                NewTopic newTopic = new NewTopic(inventoryUpdateTopic, 1, (short) 1);
                admin.createTopics(List.of(newTopic));
                logger.log("Kafka topic " + inventoryUpdateTopic + " created", LogLevel.DEBUG);
            }
            KafkaProducer<String, String> producer = createProducer();
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(inventoryUpdateTopic, payload);
            producer.send(producerRecord);
            producer.flush();

            return "success";
        } catch (Exception e) {
            logger.log("Exception: " + e, LogLevel.ERROR);
            return "failure";
        }
    }

    public KafkaProducer<String, String> createProducer() {
        if (producer == null) {
            return new KafkaProducer<>(getKafkaProperties());
        }
        return producer;
    }

    public static Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
