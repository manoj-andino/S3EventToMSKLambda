package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import xml.InventoryList;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

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
            Inventory inventory = getUploadedInventoryData(s3Event);
            createTopicIfNotPresent(logger);
            KafkaProducer<String, String> producer = createProducer();
            Optional.ofNullable(inventory)
                .map(Inventory::inventoryList)
                .map(InventoryList::records)
                .ifPresent(records ->
                    records.forEach(record -> {
                        try {
                            String key = String.valueOf(record.productId());
                            String payload = MapperUtil.writeAsString(record);
                            logger.log("Payload for " + key + " : " + payload);

                            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, payload);
                            producer.send(producerRecord);
                        } catch (Exception e) {
                            logger.log("Exception while publishing message " + e);
                        }
                    })
                );
            producer.flush();
            producer.close();
            logger.log("Published all messages to topic " + topicName);

            return "success";
        } catch (Exception e) {
            logger.log("Exception while handling request: " + e, LogLevel.ERROR);
            return "failure";
        }
    }

    private static Inventory getUploadedInventoryData(S3Event s3Event) throws JsonProcessingException {
        List<S3EventNotification.S3EventNotificationRecord> s3EventRecords = s3Event.getRecords();
        S3EventNotification.S3EventNotificationRecord s3EventNotificationRecord = s3EventRecords.getFirst();
        String bucketName = s3EventNotificationRecord.getS3().getBucket().getName();
        String objectKey = s3EventNotificationRecord.getS3().getObject().getKey();
        AmazonS3 amazonS3Client = AmazonS3ClientBuilder.defaultClient();
        String objectAsString = amazonS3Client.getObjectAsString(bucketName, objectKey);
        return MapperUtil.mapXmlToInventoryObject(objectAsString);
    }

    private static void createTopicIfNotPresent(LambdaLogger logger) throws InterruptedException, ExecutionException {
        AdminClient admin = AdminClient.create(getKafkaProperties());
        ListTopicsResult listTopics = admin.listTopics();
        Set<String> names = listTopics.names().get();
        logger.log("Kafka topics: " + names);
        if (!names.contains(topicName)) {
            NewTopic newTopic = new NewTopic(topicName, numberOfPartitions, replicationFactor);
            admin.createTopics(List.of(newTopic));
            logger.log("Kafka topic " + topicName + " created");
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
