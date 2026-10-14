import { validate } from './env.validation';

describe('Config Validation', () => {
  it('should pass with valid full configuration', () => {
    const config = {
      PORT: '3000',
      SERVICE_NAME: 'search-service',
      APP_ENV: 'development',
      APP_VERSION: '1.0.0',
      ES_NODE: 'http://localhost:9200',
      ES_INDEX_PREFIX: 'test-',
      KAFKA_BROKERS: 'localhost:9092,localhost:9093',
      KAFKA_CLIENT_ID: 'search-test',
      KAFKA_GROUP_ID: 'search-test-group',
      KAFKA_TOPIC_PREFIX: 'test.',
    };

    const validated = validate(config);
    expect(validated.PORT).toBe(3000);
    expect(validated.SERVICE_NAME).toBe('search-service');
    expect(validated.ES_NODE).toBe('http://localhost:9200');
    expect(validated.ES_INDEX_PREFIX).toBe('test-');
    expect(validated.KAFKA_BROKERS).toBe('localhost:9092,localhost:9093');
  });

  it('should use default values when optional fields are omitted', () => {
    const config = {
      ES_NODE: 'http://localhost:9200',
      KAFKA_BROKERS: 'localhost:9092',
    };

    const validated = validate(config);
    expect(validated.PORT).toBe(3000);
    expect(validated.SERVICE_NAME).toBe('search-service');
    expect(validated.APP_ENV).toBe('development');
    expect(validated.APP_VERSION).toBe('1.0.0');
    expect(validated.ES_INDEX_PREFIX).toBe('');
    expect(validated.KAFKA_TOPIC_PREFIX).toBe('');
    expect(validated.KAFKA_CONSUMER_ENABLED).toBe(true);
  });

  it('should parse KAFKA_CONSUMER_ENABLED boolean and string values correctly', () => {
    const baseConfig = {
      ES_NODE: 'http://localhost:9200',
      KAFKA_BROKERS: 'localhost:9092',
    };

    expect(
      validate({ ...baseConfig, KAFKA_CONSUMER_ENABLED: 'false' })
        .KAFKA_CONSUMER_ENABLED,
    ).toBe(false);

    expect(
      validate({ ...baseConfig, KAFKA_CONSUMER_ENABLED: false })
        .KAFKA_CONSUMER_ENABLED,
    ).toBe(false);

    expect(
      validate({ ...baseConfig, KAFKA_CONSUMER_ENABLED: 'true' })
        .KAFKA_CONSUMER_ENABLED,
    ).toBe(true);

    expect(
      validate({ ...baseConfig, KAFKA_CONSUMER_ENABLED: true })
        .KAFKA_CONSUMER_ENABLED,
    ).toBe(true);
  });

  it('should throw fail-fast error when KAFKA_CONSUMER_ENABLED is invalid', () => {
    const config = {
      ES_NODE: 'http://localhost:9200',
      KAFKA_BROKERS: 'localhost:9092',
      KAFKA_CONSUMER_ENABLED: 'not-a-boolean',
    };

    expect(() => validate(config)).toThrow(
      /Config validation error \(fail-fast\)/,
    );
  });

  it('should throw fail-fast error when ES_NODE is missing', () => {
    const config = {
      KAFKA_BROKERS: 'localhost:9092',
    };

    expect(() => validate(config)).toThrow(
      /Config validation error \(fail-fast\)/,
    );
  });

  it('should throw fail-fast error when KAFKA_BROKERS is missing', () => {
    const config = {
      ES_NODE: 'http://localhost:9200',
    };

    expect(() => validate(config)).toThrow(
      /Config validation error \(fail-fast\)/,
    );
  });
});
