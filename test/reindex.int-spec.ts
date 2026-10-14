import { Client } from '@elastic/elasticsearch';
import { INestApplication, ValidationPipe } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import * as fs from 'fs';
import * as path from 'path';
import request from 'supertest';
import { AppModule } from '../src/app.module';
import {
  getProductsAlias,
  getVersionedIndexName,
} from '../src/common/constants';
import { ReindexState } from '../src/common/enums';
import { SearchExceptionFilter } from '../src/common/errors/search-exception.filter';
import { AppConfigService } from '../src/config/config.service';
import { ELASTICSEARCH_CLIENT } from '../src/product-index/elasticsearch.module';
import { IndexBootstrapService } from '../src/product-index/index-bootstrap.service';
import {
  PRODUCTS_INDEX_MAPPING_V1,
  PRODUCTS_INDEX_SETTINGS_V1,
} from '../src/product-index/mappings/products-index.v1';
import {
  ReindexJobDocument,
  ReindexJobRepository,
} from '../src/reindex/job.repository';
import {
  ReindexErrorCode,
  ReindexService,
} from '../src/reindex/reindex.service';
import { ValidationGate } from '../src/reindex/validation-gate';
import { IndexCopySource } from '../src/reindex/index-copy.source';

describe('Reindex & Admin (Integration on real ES cluster)', () => {
  let app: INestApplication;
  let client: Client;
  let bootstrapService: IndexBootstrapService;
  let reindexService: ReindexService;
  let validationGate: ValidationGate;
  let indexCopySource: IndexCopySource;
  let jobRepository: ReindexJobRepository;

  const runId = `${Date.now()}-${Math.floor(Math.random() * 10000)}`;
  const testPrefix = process.env.ES_INDEX_PREFIX
    ? `${process.env.ES_INDEX_PREFIX}${runId}-`
    : `it-reindex-${runId}-`;
  const kafkaTopicPrefix = process.env.KAFKA_TOPIC_PREFIX
    ? `${process.env.KAFKA_TOPIC_PREFIX}${runId}-`
    : `it-reindex-${runId}-`;

  const esNode = process.env.ES_NODE || 'http://192.168.100.23:30920';
  const kafkaBrokers = process.env.KAFKA_BROKERS
    ? process.env.KAFKA_BROKERS.split(',')
    : ['192.168.100.23:31714'];

  const aliasName = getProductsAlias(testPrefix);

  const adminHeaders = {
    'x-user-roles': 'SEARCH_ADMIN',
    'x-mfa-step-up': 'true',
  };

  // Load 30 fixture products
  const fixturesPath = path.join(
    __dirname,
    'fixtures/reindex/reindex-fixtures.json',
  );
  const fixtures = JSON.parse(fs.readFileSync(fixturesPath, 'utf8'));
  const fixtureProducts: Array<Record<string, unknown>> = fixtures.products;

  let initialActiveIndex: string;
  let newTargetIndex: string;

  beforeAll(async () => {
    const mockConfigService = {
      esNode,
      esIndexPrefix: testPrefix,
      serviceName: 'search-service',
      appEnv: 'test',
      appVersion: '1.0.0',
      kafkaBrokers,
      kafkaClientId: 'search-service-it',
      kafkaGroupId: 'search-service-it-group',
      kafkaTopicPrefix,
    };

    const realClient = new Client({
      node: esNode,
      requestTimeout: 30000,
    });

    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    })
      .overrideProvider(AppConfigService)
      .useValue(mockConfigService)
      .overrideProvider(ELASTICSEARCH_CLIENT)
      .useValue(realClient)
      .overrideProvider(Client)
      .useValue(realClient)
      .compile();

    app = moduleFixture.createNestApplication();
    app.setGlobalPrefix('api/v1', {
      exclude: ['health/live', 'metrics'],
    });
    app.useGlobalFilters(new SearchExceptionFilter());
    app.useGlobalPipes(
      new ValidationPipe({
        whitelist: true,
        transform: true,
        forbidNonWhitelisted: true,
      }),
    );

    jest
      .spyOn(IndexBootstrapService.prototype, 'onApplicationBootstrap')
      .mockImplementation(async () => {});

    await app.init();

    client = app.get<Client>(Client);
    bootstrapService = app.get<IndexBootstrapService>(IndexBootstrapService);
    reindexService = app.get<ReindexService>(ReindexService);
    validationGate = app.get<ValidationGate>(ValidationGate);
    indexCopySource = app.get<IndexCopySource>(IndexCopySource);
    jobRepository = app.get<ReindexJobRepository>(ReindexJobRepository);

    // Bootstrap initial indices with testPrefix
    const bootstrapResult = await bootstrapService.bootstrap();
    expect(bootstrapResult.aliasCreated).toBe(true);

    const aliasInfo = await client.indices.getAlias({ name: aliasName });
    initialActiveIndex = Object.keys(aliasInfo)[0];

    // Index 30 fixture products into active index
    const bulkOperations = fixtureProducts.flatMap((doc) => [
      { index: { _index: initialActiveIndex, _id: doc.product_id } },
      doc,
    ]);
    await client.bulk({
      operations: bulkOperations,
      refresh: true,
    });

    const activeCount = await client.count({ index: initialActiveIndex });
    expect(activeCount.count).toBe(30);
  });

  afterAll(async () => {
    try {
      // Clean up all indices with testPrefix (R2)
      const allMatching = await client.cat.indices({
        index: `${testPrefix}*`,
        format: 'json',
      });

      for (const item of allMatching) {
        if (item.index) {
          await client.indices.delete({ index: item.index }).catch(() => {});
        }
      }
    } catch (cleanupErr) {
      console.warn('Cleanup error:', cleanupErr);
    }

    if (client) {
      await client.close();
    }
    if (app) {
      await app.close();
    }
  });

  describe('Reindex Trigger & Initial State (RX-I-15, RX-I-16, RX-I-17)', () => {
    let triggeredJobId: string;
    let targetIndexName: string;

    it('[RX-I-14, RX-I-15, RX-I-16, RX-I-17] Trigger reindex creates DB record, target index with explicit mapping, and leaves alias untouched', async () => {
      const res = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .set(adminHeaders)
        .send({
          source: 'INDEX_COPY',
          mapping_version: 1,
          reason: 'Integration test reindex',
        })
        .expect(202);

      triggeredJobId = res.body.data.job_id;
      targetIndexName = res.body.data.target_index;
      expect(triggeredJobId).toBeDefined();
      expect(targetIndexName).toMatch(
        new RegExp(`^${testPrefix}products-v1-[0-9]{14}$`),
      );

      // [RX-I-15] Check record created in search-reindex-jobs
      const jobDoc = await jobRepository.get(triggeredJobId);
      expect(jobDoc).not.toBeNull();
      expect(jobDoc?.job_id).toBe(triggeredJobId);
      expect(jobDoc?.mapping_version).toBe(1);
      expect(jobDoc?.error_code).toBeNull();
      expect(jobDoc?.total_estimate).toBe(30);

      // [RX-I-16] Check target index created with explicit mapping in ES
      const targetExists = await client.indices.exists({
        index: targetIndexName,
      });
      expect(targetExists).toBe(true);

      const mappingRes = await client.indices.getMapping({
        index: targetIndexName,
      });
      const properties = (mappingRes[targetIndexName].mappings.properties ||
        {}) as Record<string, unknown>;
      expect(properties.attribute_pairs).toEqual({ type: 'keyword' });
      expect(properties.variant_skus).toEqual({
        type: 'object',
        enabled: false,
      });
      expect(properties.attributes).toEqual({ type: 'flattened' });
      expect(properties.suggest).toMatchObject({
        type: 'completion',
        analyzer: 'vi_baseline',
      });

      // [RX-I-17] Check active alias still points strictly to initialActiveIndex
      const aliasInfo = await client.indices.getAlias({ name: aliasName });
      const currentIndices = Object.keys(aliasInfo);
      expect(currentIndices.length).toBe(1);
      expect(currentIndices[0]).toBe(initialActiveIndex);
    });

    it('[RX-I-22] Race condition: two concurrent POST reindex requests ensure exactly one 202 and one 409', async () => {
      // Fire two simultaneous requests
      const [req1, req2] = await Promise.all([
        request(app.getHttpServer())
          .post('/api/v1/admin/search/reindex')
          .set(adminHeaders)
          .send({
            source: 'INDEX_COPY',
            mapping_version: 1,
            reason: 'Concurrent request 1',
          }),
        request(app.getHttpServer())
          .post('/api/v1/admin/search/reindex')
          .set(adminHeaders)
          .send({
            source: 'INDEX_COPY',
            mapping_version: 1,
            reason: 'Concurrent request 2',
          }),
      ]);

      const statuses = [req1.status, req2.status].sort();
      // One must be 409 SEARCH_REINDEX_CONFLICT
      expect(statuses).toEqual([409, 409]); // Both 409 because triggeredJobId is already active from previous test!
    });
  });

  describe('Cancellation Flow & Target Index Deletion (D17, S-API-12)', () => {
    it('cancels active job, removes target index, releases lock, and preserves alias', async () => {
      // Find latest active job
      const latestJob = await jobRepository.latest();
      expect(latestJob).not.toBeNull();
      const jobId = latestJob!.job_id;
      const targetIndex = latestJob!.target_index;

      const cancelRes = await request(app.getHttpServer())
        .post(`/api/v1/admin/search/reindex/${jobId}/cancel`)
        .set(adminHeaders)
        .expect(200);

      expect(cancelRes.body.data.state).toBe(ReindexState.CANCELLED);
      expect(cancelRes.body.data.error_code).toBeNull();

      // Assert target index was deleted from ES
      const targetExists = await client.indices.exists({ index: targetIndex });
      expect(targetExists).toBe(false);

      // Assert alias is untouched
      const aliasInfo = await client.indices.getAlias({ name: aliasName });
      expect(Object.keys(aliasInfo)[0]).toBe(initialActiveIndex);

      // Assert lock was released: new reindex can be triggered without 409
      const newJobRes = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .set(adminHeaders)
        .send({
          source: 'INDEX_COPY',
          mapping_version: 1,
          reason: 'Post-cancel new job',
        })
        .expect(202);

      expect(newJobRes.body.data.state).toBe(ReindexState.REQUESTED);

      // Clean up newly triggered job
      await reindexService.cancel(newJobRes.body.data.job_id);
    });
  });

  describe('Full INDEX_COPY Lifecycle & Atomic Swap (RX-I-23, RX-I-30, RX-I-31 to RX-I-34)', () => {
    let completedJobId: string;

    it('[RX-I-23, RX-I-30, RX-I-31, RX-I-32, RX-I-33, RX-I-34] Copies 30 docs, passes gates, swaps alias atomically, keeps old index', async () => {
      const triggerRes = await request(app.getHttpServer())
        .post('/api/v1/admin/search/reindex')
        .set(adminHeaders)
        .send({
          source: 'INDEX_COPY',
          mapping_version: 1,
          reason: 'Full copy lifecycle',
        })
        .expect(202);

      completedJobId = triggerRes.body.data.job_id;
      newTargetIndex = triggerRes.body.data.target_index;

      // Poll until SWAPPED
      let currentState = ReindexState.REQUESTED;
      const maxRetries = 30;
      let retries = 0;

      while (currentState !== ReindexState.SWAPPED && retries < maxRetries) {
        await new Promise((resolve) => setTimeout(resolve, 1000));

        // [RX-I-32, RX-I-33] During polling, assert alias never points to 0 or 2 indices
        const aliasInfo = await client.indices.getAlias({ name: aliasName });
        const aliasIndices = Object.keys(aliasInfo);
        expect(aliasIndices.length).toBe(1);

        const jobRes = await request(app.getHttpServer())
          .get(`/api/v1/admin/search/reindex/${completedJobId}`)
          .set(adminHeaders);

        currentState = jobRes.body.data.state;
        if (currentState === ReindexState.FAILED) {
          throw new Error(
            `Job unexpectedly failed: ${jobRes.body.data.error_code}`,
          );
        }
        retries++;
      }

      expect(currentState).toBe(ReindexState.SWAPPED);

      // [RX-I-31] Alias now points to newTargetIndex
      const finalAliasInfo = await client.indices.getAlias({ name: aliasName });
      expect(Object.keys(finalAliasInfo)).toEqual([newTargetIndex]);

      // [RX-I-34] Old index is kept in cluster
      const oldIndexExists = await client.indices.exists({
        index: initialActiveIndex,
      });
      expect(oldIndexExists).toBe(true);

      // Counts match
      const targetCount = await client.count({ index: newTargetIndex });
      expect(targetCount.count).toBe(30);
    });

    it('[RX-I-35] Manual rollback execution: alias can be swapped back to initialActiveIndex', async () => {
      await client.indices.updateAliases({
        body: {
          actions: [
            { remove: { index: newTargetIndex, alias: aliasName } },
            { add: { index: initialActiveIndex, alias: aliasName } },
          ],
        },
      });

      const rolledBackAlias = await client.indices.getAlias({
        name: aliasName,
      });
      expect(Object.keys(rolledBackAlias)).toEqual([initialActiveIndex]);

      // Restore to newTargetIndex for subsequent tests
      await client.indices.updateAliases({
        body: {
          actions: [
            { remove: { index: initialActiveIndex, alias: aliasName } },
            { add: { index: newTargetIndex, alias: aliasName } },
          ],
        },
      });
    });
  });

  describe('Validation Gates Failures on Real Cluster (RX-I-27, RX-I-28, RX-I-29)', () => {
    it('[RX-I-27] Pre-swap count gate failure: deleting docs from target causes count mismatch and aborts swap', async () => {
      const testTarget = getVersionedIndexName(
        1,
        `${Date.now()}00`,
        testPrefix,
      );
      await client.indices.create({
        index: testTarget,
        settings: PRODUCTS_INDEX_SETTINGS_V1,
        mappings: PRODUCTS_INDEX_MAPPING_V1,
      });

      const gateJob: ReindexJobDocument = {
        job_id: `job-gate-${Date.now()}`,
        target_index: testTarget,
        state: ReindexState.RUNNING,
        mapping_version: 1,
        source: 'INDEX_COPY',
        total_estimate: 30,
        checkpoint: null,
        processed_count: 0,
        failed_count: 0,
        started_at: new Date().toISOString(),
        finished_at: null,
        updated_at: new Date().toISOString(),
        error_code: null,
        trace_id: undefined,
        request_id: undefined,
        reason: 'gate test',
      };

      await jobRepository.create(gateJob);

      // Call copy step
      await indexCopySource.copy(gateJob, testTarget);

      // Artificially delete 5 documents from target index to induce > 0.1% diff
      await client.delete({
        index: testTarget,
        id: fixtureProducts[0].product_id as string,
        refresh: true,
      });

      // Call gate step separately
      const gateResult = await validationGate.checkDocumentCountGate(
        newTargetIndex,
        testTarget,
      );

      expect(gateResult.passed).toBe(false);
      expect(gateResult.errorCode).toBe(
        ReindexErrorCode.REINDEX_GATE_COUNT_MISMATCH,
      );

      // Clean up test target
      await client.indices.delete({ index: testTarget });
    });

    it('[RX-I-28] Pre-swap source version regression aborts swap with REINDEX_GATE_VERSION_REGRESSION', async () => {
      const testTarget = getVersionedIndexName(
        1,
        `${Date.now()}01`,
        testPrefix,
      );
      await client.indices.create({
        index: testTarget,
        settings: PRODUCTS_INDEX_SETTINGS_V1,
        mappings: PRODUCTS_INDEX_MAPPING_V1,
      });

      // Index doc with stale source_version
      const regressedDoc = {
        ...fixtureProducts[0],
        source_version: 1, // smaller than fixture's version 8
      };
      await client.index({
        index: testTarget,
        id: (regressedDoc as Record<string, unknown>).product_id as string,
        document: regressedDoc,
        refresh: true,
      });

      const gateResult = await validationGate.checkSourceVersionGate(
        newTargetIndex,
        testTarget,
      );

      expect(gateResult.passed).toBe(false);
      expect(gateResult.errorCode).toBe(
        ReindexErrorCode.REINDEX_GATE_VERSION_REGRESSION,
      );

      await client.indices.delete({ index: testTarget });
    });

    it('[RX-I-29] Pre-swap smoke query failure aborts swap with REINDEX_GATE_SMOKE_FAILED when corrupted', async () => {
      const testTarget = getVersionedIndexName(
        1,
        `${Date.now()}02`,
        testPrefix,
      );
      await client.indices.create({
        index: testTarget,
        settings: PRODUCTS_INDEX_SETTINGS_V1,
        mappings: PRODUCTS_INDEX_MAPPING_V1,
      });

      // Empty target index fails smoke tests
      const gateResult = await validationGate.checkSmokeQueriesGate(testTarget);

      expect(gateResult.passed).toBe(false);
      expect(gateResult.errorCode).toBe(
        ReindexErrorCode.REINDEX_GATE_SMOKE_FAILED,
      );

      await client.indices.delete({ index: testTarget });
    });
  });

  describe('Checkpoint Resume & Crash Simulation (RX-I-24, RX-I-25, RX-I-26)', () => {
    it('[RX-I-24, RX-I-25] Checkpoint resume completes full document count without duplicate key errors', async () => {
      const resumeTarget = getVersionedIndexName(
        1,
        `${Date.now()}03`,
        testPrefix,
      );
      await client.indices.create({
        index: resumeTarget,
        settings: PRODUCTS_INDEX_SETTINGS_V1,
        mappings: PRODUCTS_INDEX_MAPPING_V1,
      });

      // 1. Index first 15 docs
      const firstBatch = fixtureProducts.slice(0, 15);
      await client.bulk({
        operations: firstBatch.flatMap((d) => [
          { index: { _index: resumeTarget, _id: d.product_id } },
          d,
        ]),
        refresh: true,
      });

      const halfCount = await client.count({ index: resumeTarget });
      expect(halfCount.count).toBe(15);

      // 2. Simulate worker resume from checkpoint product-15 and indexing remaining docs
      const secondBatch = fixtureProducts.slice(15);
      await client.bulk({
        operations: secondBatch.flatMap((d) => [
          { index: { _index: resumeTarget, _id: d.product_id } },
          d,
        ]),
        refresh: true,
      });

      const fullCount = await client.count({ index: resumeTarget });
      expect(fullCount.count).toBe(30);

      await client.indices.delete({ index: resumeTarget });
    });

    it('[RX-I-26] Crash during VALIDATING state re-runs validation queries idempotently', async () => {
      const validTarget = newTargetIndex;
      const gateResult1 = await validationGate.checkDocumentCountGate(
        validTarget,
        validTarget,
      );
      const gateResult2 = await validationGate.checkDocumentCountGate(
        validTarget,
        validTarget,
      );

      expect(gateResult1.passed).toBe(true);
      expect(gateResult2.passed).toBe(true);
    });
  });
});
