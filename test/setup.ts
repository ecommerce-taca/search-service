import 'reflect-metadata';

process.env.APP_ENV = process.env.APP_ENV || 'test';
process.env.ES_NODE = process.env.ES_NODE || 'http://localhost:9200';
process.env.KAFKA_BROKERS = process.env.KAFKA_BROKERS || 'localhost:9092';
