import 'reflect-metadata';

process.env.APP_ENV = process.env.APP_ENV || 'test';
process.env.ES_NODE = process.env.ES_NODE || 'http://192.168.100.23:30920';
process.env.KAFKA_BROKERS = process.env.KAFKA_BROKERS || '127.0.0.1:9092';
process.env.KAFKA_CONSUMER_ENABLED = 'false';
