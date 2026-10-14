import { Client } from '@elastic/elasticsearch';
import { Admin, Kafka, Producer } from 'kafkajs';
import {
  ES_REQUEST_TIMEOUT_MS,
  getConsumerStateIndex,
  getKafkaTopic,
  getProductsAlias,
  getSearchCategoriesIndex,
  getVersionedIndexName,
  KAFKA_TOPICS,
  MAPPING_VERSION,
} from '../src/common/constants';
import { VisibilityStatus } from '../src/common/enums';
import { AppConfigService } from '../src/config/config.service';
import { KafkaEventConsumer } from '../src/event-consumer/consumer/kafka-event.consumer';
import {
  CONSUMER_STATE_MAPPING_V1,
  PRODUCTS_INDEX_MAPPING_V1,
  PRODUCTS_INDEX_SETTINGS_V1,
  SEARCH_CATEGORIES_MAPPING_V1,
} from '../src/product-index/mappings';

import categoriesFixture from './fixtures/indexing/categories.json';
import orderingDuplicatesFixture from './fixtures/indexing/ordering-duplicates.json';
import poisonEventsFixture from './fixtures/indexing/poison-events.json';
import productsFixture from './fixtures/indexing/products.json';
import ratingsFixture from './fixtures/indexing/ratings.json';
import skusFixture from './fixtures/indexing/skus.json';

describe('Indexing Module (Integration on real ES + Kafka cluster)', () => {
  let esClient: Client;
  let kafka: Kafka;
  let kafkaAdmin: Admin;
  let kafkaProducer: Producer;
  let kafkaConsumerService: KafkaEventConsumer;

  const rand = Math.random().toString(36).substring(2, 8);
  const esPrefix = process.env.ES_INDEX_PREFIX
    ? `${process.env.ES_INDEX_PREFIX}${rand}-`
    : `it-indexing-${rand}-`;
  const kafkaPrefix = process.env.KAFKA_TOPIC_PREFIX
    ? `${process.env.KAFKA_TOPIC_PREFIX}${rand}-`
    : `it-indexing-${rand}-`;

  const versionedProductsIndex = getVersionedIndexName(
    MAPPING_VERSION,
    undefined,
    esPrefix,
  );
  const productsAlias = getProductsAlias(esPrefix);
  const categoriesIndex = getSearchCategoriesIndex(esPrefix);
  const consumerStateIndex = getConsumerStateIndex(esPrefix);

  const topics = [
    getKafkaTopic(KAFKA_TOPICS.PRODUCT_EVENTS, kafkaPrefix),
    getKafkaTopic(KAFKA_TOPICS.SKU_EVENTS, kafkaPrefix),
    getKafkaTopic(KAFKA_TOPICS.CATEGORY_EVENTS, kafkaPrefix),
    getKafkaTopic(KAFKA_TOPICS.CATALOG_EVENTS, kafkaPrefix),
    getKafkaTopic(KAFKA_TOPICS.RATING_EVENTS, kafkaPrefix),
    getKafkaTopic(KAFKA_TOPICS.DLQ, kafkaPrefix),
  ];

  const configServiceMock: Partial<AppConfigService> = {
    esNode: process.env.ES_NODE || 'http://192.168.100.23:30920',
    esIndexPrefix: esPrefix,
    kafkaBrokers: process.env.KAFKA_BROKERS
      ? process.env.KAFKA_BROKERS.split(',')
      : ['192.168.100.23:31714'],
    kafkaClientId: `it-indexing-client-${rand}`,
    kafkaGroupId: `it-indexing-group-${rand}`,
    kafkaTopicPrefix: kafkaPrefix,
    kafkaConsumerEnabled: true,
  };

  beforeAll(async () => {
    // 1. Setup Elasticsearch client and indices
    esClient = new Client({
      node: configServiceMock.esNode,
      requestTimeout: ES_REQUEST_TIMEOUT_MS,
    });

    // Create versioned products index and alias
    await esClient.indices.create({
      index: versionedProductsIndex,
      settings: PRODUCTS_INDEX_SETTINGS_V1,
      mappings: PRODUCTS_INDEX_MAPPING_V1,
    });

    await esClient.indices.putAlias({
      index: versionedProductsIndex,
      name: productsAlias,
    });

    // Create search-categories projection index
    await esClient.indices.create({
      index: categoriesIndex,
      mappings: SEARCH_CATEGORIES_MAPPING_V1,
    });

    // Create search-consumer-state index
    await esClient.indices.create({
      index: consumerStateIndex,
      mappings: CONSUMER_STATE_MAPPING_V1,
    });

    // 2. Setup Kafka client, create prefixed topics, connect producer
    kafka = new Kafka({
      clientId: configServiceMock.kafkaClientId as string,
      brokers: configServiceMock.kafkaBrokers as string[],
    });

    kafkaAdmin = kafka.admin();
    await kafkaAdmin.connect();
    await kafkaAdmin.createTopics({
      topics: topics.map((t) => ({ topic: t, numPartitions: 1 })),
      waitForLeaders: true,
    });

    kafkaProducer = kafka.producer();
    await kafkaProducer.connect();

    // 3. Initialize and start KafkaEventConsumer
    kafkaConsumerService = new KafkaEventConsumer(
      kafka,
      esClient,
      configServiceMock as AppConfigService,
    );
    await kafkaConsumerService.onApplicationBootstrap();
  }, 60000);

  afterAll(async () => {
    // Stop consumer
    try {
      if (kafkaConsumerService) {
        await kafkaConsumerService.onApplicationShutdown();
      }
    } catch (err) {
      console.warn('Error stopping consumer:', err);
    }

    // Disconnect Kafka producer
    try {
      if (kafkaProducer) {
        await kafkaProducer.disconnect();
      }
    } catch (err) {
      console.warn('Error disconnecting producer:', err);
    }

    // Clean up Kafka topics (R2)
    try {
      if (kafkaAdmin) {
        await kafkaAdmin.deleteTopics({ topics });
        await kafkaAdmin.disconnect();
      }
    } catch (err) {
      console.warn('Error cleaning up Kafka topics:', err);
    }

    // Clean up Elasticsearch indices (R2)
    try {
      if (esClient) {
        for (const idx of [
          versionedProductsIndex,
          categoriesIndex,
          consumerStateIndex,
        ]) {
          const exists = await esClient.indices.exists({ index: idx });
          if (exists) {
            await esClient.indices.delete({ index: idx });
          }
        }
        await esClient.close();
      }
    } catch (err) {
      console.warn('Error cleaning up ES indices:', err);
    }
  }, 60000);

  const waitForCondition = async (
    checkFn: () => Promise<boolean>,
    timeoutMs = 15000,
    pollIntervalMs = 500,
  ): Promise<void> => {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      if (await checkFn()) {
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
    }
    throw new Error(`Timeout waiting for condition after ${timeoutMs}ms`);
  };

  describe('[IDX-INT-01] Category tree projection seeding', () => {
    it('[IDX-INT-01] should produce category events and verify projection docs in search-categories index', async () => {
      const topic = getKafkaTopic(KAFKA_TOPICS.CATEGORY_EVENTS, kafkaPrefix);

      for (const catEvent of categoriesFixture) {
        await kafkaProducer.send({
          topic,
          messages: [
            {
              key: catEvent.aggregate_id,
              value: JSON.stringify(catEvent),
            },
          ],
        });
      }

      // Wait until leaf category exists in ES projection
      await waitForCondition(async () => {
        try {
          const res = await esClient.get({
            index: categoriesIndex,
            id: 'cat-leaf-smartphones',
          });
          return res.found;
        } catch {
          return false;
        }
      });

      const leafDoc = await esClient.get({
        index: categoriesIndex,
        id: 'cat-leaf-smartphones',
      });

      expect(leafDoc.found).toBe(true);
      expect((leafDoc._source as { path_ids: string[] }).path_ids).toEqual([
        'cat-root-electronics',
        'cat-child-phones',
        'cat-leaf-smartphones',
      ]);
    });
  });

  describe('[IDX-INT-02] PUBLISHED full product with 2 SKUs', () => {
    it('[IDX-INT-02] should process product and SKU events, producing indexed doc with aggregated attributes and VND price', async () => {
      const prodTopic = getKafkaTopic(KAFKA_TOPICS.PRODUCT_EVENTS, kafkaPrefix);
      const skuTopic = getKafkaTopic(KAFKA_TOPICS.SKU_EVENTS, kafkaPrefix);

      // Publish product created & published events
      const createEvt = productsFixture.find(
        (e) => e.event_id === '01912f70-prod-001-created',
      );
      const pubEvt = productsFixture.find(
        (e) => e.event_id === '01912f70-prod-001-published',
      );

      if (createEvt) {
        await kafkaProducer.send({
          topic: prodTopic,
          messages: [
            { key: createEvt.aggregate_id, value: JSON.stringify(createEvt) },
          ],
        });
      }
      if (pubEvt) {
        await kafkaProducer.send({
          topic: prodTopic,
          messages: [
            { key: pubEvt.aggregate_id, value: JSON.stringify(pubEvt) },
          ],
        });
      }

      // Produce active SKUs
      const sku1 = skusFixture.find((e) => e.event_id === '01912f70-sku-001-1');
      const sku2 = skusFixture.find((e) => e.event_id === '01912f70-sku-001-2');

      if (sku1) {
        await kafkaProducer.send({
          topic: skuTopic,
          messages: [{ key: sku1.aggregate_id, value: JSON.stringify(sku1) }],
        });
      }
      if (sku2) {
        await kafkaProducer.send({
          topic: skuTopic,
          messages: [{ key: sku2.aggregate_id, value: JSON.stringify(sku2) }],
        });
      }

      // Wait until consumer state records processed
      await waitForCondition(async () => {
        try {
          const res = await esClient.get({
            index: versionedProductsIndex,
            id: 'prod-001-published',
          });
          const source = res._source as {
            visibility_status?: string;
            attributes?: { color?: string[] };
          };
          return (
            res.found &&
            source?.visibility_status === VisibilityStatus.PUBLISHED &&
            (source?.attributes?.color?.length ?? 0) >= 2
          );
        } catch {
          return false;
        }
      });

      const doc = await esClient.get({
        index: versionedProductsIndex,
        id: 'prod-001-published',
      });

      expect(doc.found).toBe(true);
      const source = doc._source as {
        price: number;
        currency: string;
        visibility_status: string;
        attributes: Record<string, string[]>;
        attribute_pairs: string[];
        spec_attributes: Record<string, string>;
        cover_url: string;
        shop: { shop_id: string; name: string };
      };

      expect(source.visibility_status).toBe(VisibilityStatus.PUBLISHED);
      expect(source.price).toBe(249000); // price = payload.price.sale
      expect(source.currency).toBe('VND');
      expect(source.attributes.color).toEqual(
        expect.arrayContaining(['Đen', 'Trắng']),
      );
      expect(source.attributes.size).toEqual(
        expect.arrayContaining(['M', 'L']),
      );
      expect(source.attribute_pairs).toEqual(
        expect.arrayContaining([
          'color=Đen',
          'color=Trắng',
          'size=M',
          'size=L',
        ]),
      );
      expect(source.spec_attributes).toEqual({ material: 'cotton' });
      expect(source.cover_url).toBe('https://cdn.example/coat.jpg');
      expect(source.shop).toEqual({
        shop_id: 'shop-01912f30',
        name: 'Taca Official Shop',
      });
    });
  });

  describe('[IDX-INT-03] HIDDEN product state', () => {
    it('[IDX-INT-03] should process product created, published then blocked and index visibility_status=HIDDEN', async () => {
      const prodTopic = getKafkaTopic(KAFKA_TOPICS.PRODUCT_EVENTS, kafkaPrefix);

      const pubEvt = productsFixture.find(
        (e) => e.event_id === '01912f70-prod-002-published',
      );
      const blockEvt = productsFixture.find(
        (e) => e.event_id === '01912f70-prod-002-blocked',
      );

      if (pubEvt) {
        await kafkaProducer.send({
          topic: prodTopic,
          messages: [
            { key: pubEvt.aggregate_id, value: JSON.stringify(pubEvt) },
          ],
        });
      }
      if (blockEvt) {
        await kafkaProducer.send({
          topic: prodTopic,
          messages: [
            { key: blockEvt.aggregate_id, value: JSON.stringify(blockEvt) },
          ],
        });
      }

      await waitForCondition(async () => {
        try {
          const res = await esClient.get({
            index: versionedProductsIndex,
            id: 'prod-002-hidden',
          });
          const source = res._source as { visibility_status?: string };
          return (
            res.found && source?.visibility_status === VisibilityStatus.HIDDEN
          );
        } catch {
          return false;
        }
      });

      const doc = await esClient.get({
        index: versionedProductsIndex,
        id: 'prod-002-hidden',
      });

      expect(doc.found).toBe(true);
      expect(
        (doc._source as { visibility_status: string }).visibility_status,
      ).toBe(VisibilityStatus.HIDDEN);
    });
  });

  describe('[IDX-INT-04] DELETED product state', () => {
    it('[IDX-INT-04] should process product created, published then archived and index visibility_status=DELETED', async () => {
      const prodTopic = getKafkaTopic(KAFKA_TOPICS.PRODUCT_EVENTS, kafkaPrefix);

      const pubEvt = productsFixture.find(
        (e) => e.event_id === '01912f70-prod-003-published',
      );
      const archEvt = productsFixture.find(
        (e) => e.event_id === '01912f70-prod-003-archived',
      );

      if (pubEvt) {
        await kafkaProducer.send({
          topic: prodTopic,
          messages: [
            { key: pubEvt.aggregate_id, value: JSON.stringify(pubEvt) },
          ],
        });
      }
      if (archEvt) {
        await kafkaProducer.send({
          topic: prodTopic,
          messages: [
            { key: archEvt.aggregate_id, value: JSON.stringify(archEvt) },
          ],
        });
      }

      await waitForCondition(async () => {
        try {
          const res = await esClient.get({
            index: versionedProductsIndex,
            id: 'prod-003-deleted',
          });
          const source = res._source as { visibility_status?: string };
          return (
            res.found && source?.visibility_status === VisibilityStatus.DELETED
          );
        } catch {
          return false;
        }
      });

      const doc = await esClient.get({
        index: versionedProductsIndex,
        id: 'prod-003-deleted',
      });

      expect(doc.found).toBe(true);
      expect(
        (doc._source as { visibility_status: string }).visibility_status,
      ).toBe(VisibilityStatus.DELETED);
    });
  });

  describe('[IDX-INT-05] Rating event updates', () => {
    it('[IDX-INT-05] should process rating.aggregate.updated and patch rating_avg / rating_count on product', async () => {
      const ratingTopic = getKafkaTopic(
        KAFKA_TOPICS.RATING_EVENTS,
        kafkaPrefix,
      );
      const ratingEvt = ratingsFixture.find(
        (e) => e.event_id === '01912f70-rat-001',
      );

      if (ratingEvt) {
        await kafkaProducer.send({
          topic: ratingTopic,
          messages: [
            { key: ratingEvt.aggregate_id, value: JSON.stringify(ratingEvt) },
          ],
        });
      }

      await waitForCondition(async () => {
        try {
          const res = await esClient.get({
            index: versionedProductsIndex,
            id: 'prod-001-published',
          });
          const source = res._source as { rating_avg?: number };
          return res.found && source?.rating_avg === 4.8;
        } catch {
          return false;
        }
      });

      const doc = await esClient.get({
        index: versionedProductsIndex,
        id: 'prod-001-published',
      });

      const source = doc._source as {
        rating_avg: number;
        rating_count: number;
      };
      expect(source.rating_avg).toBe(4.8);
      expect(source.rating_count).toBe(120);
    });
  });

  describe('[IDX-INT-06] Duplicate and out-of-order events resilience', () => {
    it('[IDX-INT-06] should not regress source_version when receiving stale or duplicate events', async () => {
      const prodTopic = getKafkaTopic(KAFKA_TOPICS.PRODUCT_EVENTS, kafkaPrefix);

      for (const staleEvt of orderingDuplicatesFixture) {
        await kafkaProducer.send({
          topic: prodTopic,
          messages: [
            { key: staleEvt.aggregate_id, value: JSON.stringify(staleEvt) },
          ],
        });
      }

      // Allow consumer to process
      await new Promise((resolve) => setTimeout(resolve, 2000));

      const doc = await esClient.get({
        index: versionedProductsIndex,
        id: 'prod-001-published',
      });

      const source = doc._source as {
        source_version: number;
        title: string;
      };

      // Ensure version did not regress to 1 and title did not overwrite
      expect(source.source_version).toBeGreaterThanOrEqual(2);
      expect(source.title).not.toContain('STALE VERSION');
    });
  });

  describe('[IDX-INT-07] Poison event and DLQ routing', () => {
    it('[IDX-INT-07] should route poison events to DLQ topic with redacted payload after retries', async () => {
      const prodTopic = getKafkaTopic(KAFKA_TOPICS.PRODUCT_EVENTS, kafkaPrefix);
      const dlqTopic = getKafkaTopic(KAFKA_TOPICS.DLQ, kafkaPrefix);

      // Listen on DLQ topic
      const dlqConsumer = kafka.consumer({
        groupId: `it-dlq-verifier-${rand}`,
      });
      await dlqConsumer.connect();
      await dlqConsumer.subscribe({ topic: dlqTopic, fromBeginning: true });

      const dlqMessages: unknown[] = [];
      await dlqConsumer.run({
        eachMessage: async ({ message }) => {
          if (message.value) {
            dlqMessages.push(JSON.parse(message.value.toString()));
          }
        },
      });

      // Send poison event
      const poisonEvt = poisonEventsFixture.find(
        (e) => e.event_id === '01912f70-poison-001',
      );
      if (poisonEvt) {
        await kafkaProducer.send({
          topic: prodTopic,
          messages: [
            { key: poisonEvt.aggregate_id, value: JSON.stringify(poisonEvt) },
          ],
        });
      }

      // Wait until DLQ message received
      await waitForCondition(async () => {
        return dlqMessages.length > 0;
      }, 20000);

      await dlqConsumer.disconnect();

      expect(dlqMessages.length).toBeGreaterThan(0);
      const received = dlqMessages[0] as {
        event_id?: string;
        payload?: unknown;
      };
      expect(received.event_id).toBe('01912f70-poison-001');
      expect(received.payload).toBeUndefined(); // Redacted payload
    });
  });
});
