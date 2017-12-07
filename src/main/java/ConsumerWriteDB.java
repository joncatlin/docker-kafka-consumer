import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

import rx.Observable;
import rx.functions.Func1;

public class ConsumerWriteDB {

	static final String ENV_KAFKA_NODES = "KAFKA_NODES";
	static final String ENV_COUCHBASE_NODES = "COUCHBASE_NODES";
	static final String ENV_KAFKA_TOPIC = "KAFKA_TOPIC";
	static final String ENV_KAFKA_GROUP_ID = "KAFKA_GROUP_ID";
	static final String ENV_STATSD_SERVER = "STATSD_SERVER";
	static final String ENV_BATCH_SIZE = "BATCH_SIZE";
	static final String SEPERATOR = "|";
/*
	static String kafkaNodes = null;
	static String couchbaseNodes = null;
	static String kafkaTopic = null;
	static String kafkaGroupId = null;
	static String statsdServer = null;
	static int batchSize = 1;
*/
	
	static Map<String, String> env = System.getenv();
	static List<String> envVariables = new ArrayList<String>();

	

	public static void main(String[] args) {

		// Initialize the list of Environment Variables that must exist
		envVariables.add(ENV_KAFKA_NODES);
		envVariables.add(ENV_COUCHBASE_NODES);
		envVariables.add(ENV_KAFKA_TOPIC);
		envVariables.add(ENV_KAFKA_GROUP_ID);
		envVariables.add(ENV_STATSD_SERVER);
		envVariables.add(ENV_BATCH_SIZE);
		
		// Initialize the environment
		checkEnvVariables();

		// Read in the file and send to a kafka topic
		consume();
	}

	
	private static void consume() {

		ConsumerRecords<String, String> records = null;
		KafkaConsumer<String, String> consumer = null;

		// Get the connection to the Statsd backend and wait for it to be available
		StatsDClient statsd = getStatsdConnection();

		// Get the connection to the DB and wait until it is available

		final Bucket bucket = getDBConnection();
	
		consumer = getKafkaConnection();
/*		
		// Create the connection to the Kafka cluster
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaNodes);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString()); // Use a random ID so Kafka does not
																					// remember this client
		// props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId);
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "TechdemoConsumer");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");

		@SuppressWarnings("resource")
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
*/
		// Subscribe to the list of topics
		consumer.subscribe(Arrays.asList(env.get(ENV_KAFKA_TOPIC)));

		// Process any records received from the topic and store them in the DB
		int batchCount = 0;
		int totalRecordsReceived = 0;
		while (true) {

			// Get some records from Kafka
			records = consumer.poll(100);
			int countOfRecords = records.count();

			if (countOfRecords != 0) {
				totalRecordsReceived += countOfRecords;
				System.out.println("Batch: " + batchCount + ", Total records received: " + totalRecordsReceived + ", Count of records found: " + countOfRecords);
				batchCount++;
				
				// Create the Iterator to access the records
				Iterator it = records.iterator();
				
				// Batch up a number of documents depending on batch size
				for (int itemNo = 0; itemNo < countOfRecords;) {

					// Capture the start time of the processing so the net time can be calculated
					final long startTime = System.currentTimeMillis();
			        
					// Create a batch of documents to write to the DB
					List<JsonDocument> documents = new ArrayList<JsonDocument>();
					int batchNo;
					for (batchNo = 0; batchNo < Integer.valueOf(env.get(ENV_BATCH_SIZE)) && itemNo < countOfRecords; batchNo++, itemNo++) {
						ConsumerRecord<String, String> record = (ConsumerRecord<String, String>)it.next();
						documents.add(JsonDocument.create(record.key(), JsonObject.fromJson(record.value())));
					}

					boolean retry = true;
					do {
						try {
							// Write the batch to the DB, waiting until the last one is done.
							Observable.from(documents).flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
								public Observable<JsonDocument> call(final JsonDocument docToInsert) {
									return bucket.async().upsert(docToInsert);
								}
							}).last().toBlocking().single();
							retry = false;
						} catch (TemporaryFailureException e) {
							System.out.println("Temporary failure exception thrown. Waiting and then trying again.");
							try {Thread.sleep(100);} catch (InterruptedException ex) { System.exit(0); }
						}
					} while (retry);
					
					// Capture the metrics
			        statsd.recordExecutionTimeToNow("BatchDBWriteTime", startTime);
					statsd.count("MsgsWrittenToDB", batchNo+1);
				}
			}
		}
	}

	
	private static void checkEnvVariables() {

		// Check all environment variables exist and if not exit
		for (String envVar : envVariables) {
			if (!env.containsKey(envVar)) {
				System.out.println("Missing environment variable: " + envVar);
				System.exit(0);
			} else {
				System.out.println(envVar + " = " + env.get(envVar));
			}
		}
	}
	
	
	private static Bucket getDBConnection() {

		boolean waitingForCouchbaseServer = true;
		Cluster cluster = null; 
		Bucket bucket = null; 

		do {
			try {
				// Create a connection to the DB
				CouchbaseEnvironment cbEnv = DefaultCouchbaseEnvironment.builder().connectTimeout(30000) 	// 10000ms = 10s, default
																										// is 5s
						.managementTimeout(30000) // 30s
						.socketConnectTimeout(10000) // 10s
						.build();
				cluster = CouchbaseCluster.create(cbEnv, env.get(ENV_COUCHBASE_NODES));
				
				cluster.authenticate("anyone", "anyone");
				
				
				bucket = cluster.openBucket(env.get(ENV_KAFKA_TOPIC));
				waitingForCouchbaseServer = false;
			} catch (Exception e) {
				System.out.println("DB server unavailable, waiting for connection before continuing");
				try {Thread.sleep(10000L);} catch (InterruptedException ex) { System.exit(0); }
			}
		} while (waitingForCouchbaseServer);

		System.out.println("DB server available, continuing");
		return bucket;
	}
	
	
	private static NonBlockingStatsDClient getStatsdConnection() {
		// Connect to the Statsd backend and wait for a successful connection
		boolean waitingForStatsdServer = true;
		NonBlockingStatsDClient statsd = null;
		
		do {
			try {
				statsd = new NonBlockingStatsDClient("kafka-consumer", env.get(ENV_STATSD_SERVER), 8125);
				waitingForStatsdServer = false;
			} catch (Exception e) {
				System.out.println("Statsd server unavailable, waiting for connection before continuing");
				try {Thread.sleep(10000L);} catch (InterruptedException ex) { System.exit(0); }
			}
		} while (waitingForStatsdServer);		
		
		System.out.println("Statsd server available, continuing");
		return statsd;
	}
	
	
	private static KafkaConsumer<String, String> getKafkaConnection() {

		KafkaConsumer<String, String> consumer = null;
		
		// Create the connection to the Kafka cluster
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env.get(ENV_KAFKA_NODES));
//		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString()); // Use a random ID so Kafka does not
																					// remember this client
		props.put(ConsumerConfig.GROUP_ID_CONFIG, env.get(ENV_KAFKA_GROUP_ID));
//		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "TechdemoConsumer");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");

		boolean waitingForKafkaServer = true;
		
		do {
			try {
		//		@SuppressWarnings("resource")
				consumer = new KafkaConsumer<String, String>(props);

				// Try the connection to see if it is operational by getting a list of the topics
				Map<String, List<PartitionInfo> > topics = consumer.listTopics();
				for (String topicName : topics.keySet()) {
					System.out.println("Topic found: " + topicName );
				}
				
				waitingForKafkaServer = false;
			} catch (Exception e) {
				System.out.println("Kafka server unavailable, waiting for connection before continuing");
				try {Thread.sleep(10000L);} catch (InterruptedException ex) { System.exit(0); }
			}
		} while (waitingForKafkaServer);		
		
		System.out.println("Kafka server available, continuing");
		return consumer;
	}
}
