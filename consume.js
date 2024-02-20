const Kafka = require('node-rdkafka');
const { Pool } = require('pg');
const fs = require('fs');
const v8 = require('v8');
const TOPIC_NAME = 'aiven_kafka_demo';
const host = "data-pipeline-test-dvd-rental-pg-yipintsoi-63b0.a.aivencloud.com"
const port = 22957
const username = 'avnadmin'
const password = 'AVNS_LSFYw_GbogtfyvXcEsT'
const database = "kafka_demo"

//postgres config
const POSTGRES_CONFIG = {
    user: username,
    host: host,
    database: database,
    password: password,
    port: port,
    ssl: {
        ca: fs.readFileSync('./sslcerts/ca.pem'),
        cert: fs.readFileSync('./sslcerts/service.cert'),
        key: fs.readFileSync('./sslcerts/service.key'),
    },
};

// Create a PostgreSQL pool
const pool = new Pool(POSTGRES_CONFIG);

// Create a Kafka consumer
const consumer = new Kafka.KafkaConsumer(
    {
        'group.id': 'postgres_conn',
        'metadata.broker.list': 'data-pipeline-test-dvd-rental-kafka-yipintsoi-63b0.a.aivencloud.com:22959',
        'security.protocol': 'ssl',
        'ssl.ca.location': './sslcerts/ca.pem',
        'ssl.certificate.location': './sslcerts/service.cert',
        'ssl.key.location': './sslcerts/service.key',
    },
    {
        'auto.offset.reset': 'beginning',
    }
);

// Connect to Kafka and PostgreSQL
consumer.connect();

// Handle errors
consumer.on('event.error', (err) => {
    console.error('Error from consumer:', err);
});

// Set up the consumer event handlers
consumer.on('ready', () => {
    console.log('Consumer is ready');
    // Subscribe to the Kafka topic
    consumer.subscribe([TOPIC_NAME]);
    // Start consuming messages
    consumer.consume();
})
    .on('data', async (message) => {
        try {
            const data = JSON.parse(message.value)
            await insertIntoPostgres(data.classes, data.sets, data.standard, data.wild, data.types, data.factions, data.qualities, data.races, data.locales);
            console.log('Data inserted into PostgreSQL');
        } catch (error) {
            console.error('Error processing Kafka message:', error.message);
        }
    }).on('disconnected', () => {
        console.log('Consumer disconnected');
        // Disconnect from PostgreSQL
        pool.end();
    });
// Function to insert data into PostgreSQL
async function insertIntoPostgres(classes, sets, standard, wild, types, factions, qualities, races, locales) {
    let client;

    try {
        // Connect to PostgreSQL outside the loop to use the same connection for all inserts
        client = await pool.connect();
        const query = `
            INSERT INTO public.aiven_kafka (classes, sets, standard, wild, types, factions, qualities, races, locales)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `;
        await client.query(query, [classes, sets, standard, wild, types, factions, qualities, races, locales]);
    } catch (error) {
        console.error('Error inserting data into PostgreSQL:', error.message);
    }
    finally {
        consumer.disconnect()
        client.release()
    }
}