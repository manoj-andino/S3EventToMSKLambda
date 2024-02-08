package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;
import software.amazon.msk.auth.iam.IAMLoginModule;
import utils.MapperUtil;
import xml.Inventory;

import java.util.List;
import java.util.Properties;
import java.util.Set;

public class InventoryUpdate implements RequestHandler<S3Event, String> {
    static String bootstrapServers = System.getenv("MSK_BOOTSTRAP_SERVERS");
    static String topicName = System.getenv("TOPIC_NAME");
    static int numberOfPartitions = Integer.parseInt(System.getenv("NUM_OF_PARTITIONS"));
    static short replicationFactor = Short.parseShort(System.getenv("REPLICATION_FACTOR"));
    private KafkaProducer<String, String> producer;

    @Override
    public String handleRequest(S3Event s3Event, Context context) {
        LambdaLogger logger = context.getLogger();
        try {
            List<S3EventNotification.S3EventNotificationRecord> s3EventRecords = s3Event.getRecords();
            S3EventNotification.S3EventNotificationRecord s3EventNotificationRecord = s3EventRecords.getFirst();
            logger.log("Received event: " + s3EventNotificationRecord.getEventName());

            String bucketName = s3EventNotificationRecord.getS3().getBucket().getName();
            String key = s3EventNotificationRecord.getS3().getObject().getKey();

            AmazonS3 amazonS3Client = AmazonS3ClientBuilder.defaultClient();
            String objectAsString = amazonS3Client.getObjectAsString(bucketName, key);

            Inventory inventory = MapperUtil.mapXmlToInventoryObject(objectAsString);
            String payload = MapperUtil.writeAsString(inventory);
            logger.log("Payload " + payload);

            AdminClient admin = AdminClient.create(getKafkaProperties());
            ListTopicsResult listTopics = admin.listTopics();
            Set<String> names = listTopics.names().get();
            logger.log("Kafka topics: " + names);
            if (!names.contains(topicName)) {
                NewTopic newTopic = new NewTopic(topicName, numberOfPartitions, replicationFactor);
                admin.createTopics(List.of(newTopic));
                logger.log("Kafka topic " + topicName + " created");
            }
            KafkaProducer<String, String> producer = createProducer();
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, payload);
            producer.send(producerRecord);
            producer.flush();
            logger.log("Published the message to topic " + topicName);

            return "success";
        } catch (Exception e) {
            logger.log("Exception while handling request: " + e, LogLevel.ERROR);
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

        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name());

        properties.setProperty(SaslConfigs.SASL_MECHANISM, IAMLoginModule.MECHANISM);
        properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG, "software.amazon.msk.auth.iam.IAMLoginModule required;");
        properties.setProperty(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        return properties;
    }
}
