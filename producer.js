const axios = require('axios');
const Kafka = require('node-rdkafka');

const TOPIC_NAME = 'aiven_kafka_demo';

const producer = new Kafka.Producer({
  'metadata.broker.list': 'data-pipeline-test-dvd-rental-kafka-yipintsoi-63b0.a.aivencloud.com:22959',
  'security.protocol': 'ssl',
  'ssl.ca.location': './sslcerts/ca.pem',
  'ssl.certificate.location': './sslcerts/service.cert',
  'ssl.key.location': './sslcerts/service.key',
  'dr_cb': true,
});

// Handle any errors that occur during the production process
producer.on('event.error', (err) => {
  console.error('Error from producer:', err);
});

producer.connect();

const options = {
  method: 'GET',
  url: 'https://omgvamp-hearthstone-v1.p.rapidapi.com/info',
  headers: {
    'X-RapidAPI-Key': 'cb1e10872bmshcfe748bad2b166ep11711djsn05afcda253c4',
    'X-RapidAPI-Host': 'omgvamp-hearthstone-v1.p.rapidapi.com',
  },
};

const produceMessage = async () => {
  try {
    const response = await axios(options);

    if (response.status === 200) {
      // Produce a message to the Kafka topic
      producer.produce(TOPIC_NAME, null, Buffer.from(JSON.stringify(response.data)), null, Date.now());
      // Wait for any outstanding messages to be delivered and delivery reports to be received
      producer.flush(10000);
    }
  } catch (error) {
    console.error('Error fetching data from API:', error.message);
  } finally {
    // Disconnect from Kafka after producing the message
    producer.disconnect();
  }
};

produceMessage();
