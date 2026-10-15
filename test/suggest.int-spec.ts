import { Client } from '@elastic/elasticsearch';
import { Test, TestingModule } from '@nestjs/testing';
import * as fs from 'fs';
import * as path from 'path';
import {
  ES_REQUEST_TIMEOUT_MS,
  formatUtcTimestamp,
  getProductsAlias,
  getVersionedIndexName,
} from '../src/common/constants';
import { SuggestionType } from '../src/common/enums';
import { AppConfigService } from '../src/config/config.service';
import { ELASTICSEARCH_CLIENT } from '../src/product-index/elasticsearch.module';
import {
  PRODUCTS_INDEX_MAPPING_V1,
  PRODUCTS_INDEX_SETTINGS_V1,
} from '../src/product-index/mappings';
import {
  SuggestService,
  Suggestion,
} from '../src/autocomplete/suggest.service';

describe('SuggestService (Integration on real ES cluster)', () => {
  let service: SuggestService;
  let client: Client;
  let moduleRef: TestingModule;

  const rand = Math.random().toString(36).substring(2, 8);
  const testPrefix = process.env.ES_INDEX_PREFIX
    ? `${process.env.ES_INDEX_PREFIX}${rand}-`
    : `it-suggest-${rand}-`;

  const aliasName = getProductsAlias(testPrefix);
  const concreteIndex = getVersionedIndexName(
    1,
    formatUtcTimestamp(),
    testPrefix,
  );

  const esNode =
    process.env.ES_NODE && !process.env.ES_NODE.includes('127.0.0.1')
      ? process.env.ES_NODE
      : 'http://192.168.100.23:30920';

  const mockConfigService = {
    esNode,
    esIndexPrefix: testPrefix,
    serviceName: 'search-service',
    appEnv: 'test',
    appVersion: '1.0.0',
    kafkaBrokers: process.env.KAFKA_BROKERS
      ? process.env.KAFKA_BROKERS.split(',')
      : ['192.168.100.23:31714'],
    kafkaClientId: 'search-service',
    kafkaGroupId: 'search-service-group',
    kafkaTopicPrefix: '',
    kafkaConsumerEnabled: false,
  };

  beforeAll(async () => {
    moduleRef = await Test.createTestingModule({
      providers: [
        {
          provide: AppConfigService,
          useValue: mockConfigService,
        },
        {
          provide: ELASTICSEARCH_CLIENT,
          useFactory: () => {
            const rawClient = new Client({
              node: mockConfigService.esNode,
              requestTimeout: ES_REQUEST_TIMEOUT_MS,
            });
            return rawClient;
          },
        },
        {
          provide: Client,
          useExisting: ELASTICSEARCH_CLIENT,
        },
        SuggestService,
      ],
    }).compile();

    service = moduleRef.get<SuggestService>(SuggestService);
    client = moduleRef.get<Client>(Client);

    // 1. Create versioned index with mappings and settings
    await client.indices.create({
      index: concreteIndex,
      settings: PRODUCTS_INDEX_SETTINGS_V1,
      mappings: PRODUCTS_INDEX_MAPPING_V1,
    });

    // 2. Point products alias to concrete index
    await client.indices.putAlias({
      index: concreteIndex,
      name: aliasName,
    });

    // 3. Load and index fixtures
    const fixturePath = path.resolve(
      __dirname,
      'fixtures/suggest-health/suggest-fixtures.json',
    );
    const fixtureData = JSON.parse(fs.readFileSync(fixturePath, 'utf8'));

    for (const prod of fixtureData.products) {
      const doc = {
        ...prod,
        suggest: Array.isArray(prod.suggest)
          ? prod.suggest.filter(
              (s: string) =>
                s.toLowerCase() !== String(prod.brand || '').toLowerCase(),
            )
          : prod.suggest,
      };
      await client.index({
        index: concreteIndex,
        id: prod.product_id,
        document: doc,
      });
    }

    // 4. Index Apple product with "Apple" in title so prefix=apple returns QUERY, PRODUCT, and BRAND (AC-I-06)
    await client.index({
      index: concreteIndex,
      id: 'product-apple-watch',
      document: {
        product_id: 'product-apple-watch',
        shop_id: 'shop-01912f35',
        title: 'Apple Watch Series 9 GPS 45mm nhôm chính hãng',
        title_exact: 'Apple Watch Series 9 GPS 45mm nhôm chính hãng',
        slug: 'apple-watch-series-9-gps-45mm',
        description: 'Đồng hồ thông minh Apple Watch',
        category_ids: ['category-electronics', 'category-phone'],
        category_path: ['electronics', 'phone'],
        brand: 'Apple',
        price: 9990000,
        currency: 'VND',
        rating_avg: 4.9,
        rating_count: 50,
        sales_count: 100,
        attributes: {},
        spec_attributes: {},
        suggest: ['Apple Watch Series 9 GPS 45mm nhôm chính hãng'],
        visibility_status: 'PUBLISHED',
        source_version: 1,
        source_event_id: 'evt-apple-watch',
        indexed_at: '2026-08-30T09:25:00Z',
      },
    });

    // 5. Index outerwear product with distinct >6 words title so PRODUCT is not deduped by 6-word QUERY (AC-I-01, AC-I-04, AC-I-10)
    await client.index({
      index: concreteIndex,
      id: 'product-outerwear-long',
      document: {
        product_id: 'product-outerwear-long',
        shop_id: 'shop-01912f30',
        title: 'Áo khoác gió chống nước thể thao cao cấp hai lớp mùa đông',
        title_exact:
          'Áo khoác gió chống nước thể thao cao cấp hai lớp mùa đông',
        slug: 'ao-khoac-gio-chong-nuoc-the-thao-cao-cap',
        description: 'Áo khoác cao cấp chống nước',
        category_ids: ['category-fashion', 'category-outerwear'],
        category_path: ['fashion', 'outerwear'],
        brand: 'The North Face',
        price: 850000,
        currency: 'VND',
        rating_avg: 4.8,
        rating_count: 30,
        sales_count: 90,
        attributes: {},
        spec_attributes: {},
        suggest: ['Áo khoác gió chống nước thể thao cao cấp hai lớp mùa đông'],
        visibility_status: 'PUBLISHED',
        source_version: 1,
        source_event_id: 'evt-outerwear-long',
        indexed_at: '2026-08-30T09:26:00Z',
      },
    });

    // 6. Index additional items starting with 'Ananas' to exceed default limit 10 for AC-I-07 without polluting 'ao'
    for (let i = 1; i <= 8; i++) {
      await client.index({
        index: concreteIndex,
        id: `product-ananas-${i}`,
        document: {
          product_id: `product-ananas-${i}`,
          shop_id: 'shop-01912f30',
          title: `Ananas casual sneaker classic version ${i}`,
          title_exact: `Ananas casual sneaker classic version ${i}`,
          slug: `ananas-casual-sneaker-classic-${i}`,
          description: `Giày sneaker Ananas ${i}`,
          category_ids: ['category-fashion'],
          category_path: ['fashion'],
          brand: 'Ananas',
          price: 550000 + i * 10000,
          currency: 'VND',
          rating_avg: 4.2,
          rating_count: 10,
          sales_count: 20,
          attributes: {},
          spec_attributes: {},
          suggest: [`Ananas casual sneaker classic version ${i}`],
          visibility_status: 'PUBLISHED',
          source_version: 1,
          source_event_id: `evt-ananas-${i}`,
          indexed_at: '2026-08-30T09:30:00Z',
        },
      });
    }

    // Refresh index
    await client.indices.refresh({ index: concreteIndex });
  });

  afterAll(async () => {
    try {
      const aliasExists = await client.indices.existsAlias({ name: aliasName });
      if (aliasExists) {
        await client.indices.deleteAlias({
          index: concreteIndex,
          name: aliasName,
        });
      }

      const indexExists = await client.indices.exists({ index: concreteIndex });
      if (indexExists) {
        await client.indices.delete({ index: concreteIndex });
      }
    } catch (cleanupErr) {
      console.warn('Error during ES cleanup in suggest.int-spec:', cleanupErr);
    }

    await client.close();
    await moduleRef.close();
  });

  it('AC-I-01: returns folded matches (QUERY, PRODUCT, BRAND) and excludes HIDDEN/DELETED products', async () => {
    const results: Suggestion[] = await service.suggest({
      prefix: 'ao',
      limit: 10,
    });

    expect(results.length).toBeGreaterThan(0);

    // Verify QUERY suggestion exists
    const querySuggestions = results.filter(
      (r) => r.type === SuggestionType.QUERY,
    );
    expect(querySuggestions.length).toBeGreaterThan(0);
    expect(querySuggestions.some((q) => q.text.includes('áo khoác'))).toBe(
      true,
    );

    // Verify PRODUCT suggestions exist with product_id
    const productSuggestions = results.filter(
      (r) => r.type === SuggestionType.PRODUCT,
    );
    expect(productSuggestions.length).toBeGreaterThan(0);
    const prodIds = productSuggestions.map((p) => p.product_id);
    expect(prodIds).toContain('product-outerwear-long');

    // Strictly exclude HIDDEN (product-01912f35) and DELETED (product-01912f36)
    expect(prodIds).not.toContain('product-01912f35');
    expect(prodIds).not.toContain('product-01912f36');
  });

  it('AC-I-02: accented prefix Áo returns identical match set as unaccented prefix ao', async () => {
    const unaccented = await service.suggest({ prefix: 'ao', limit: 10 });
    const accented = await service.suggest({ prefix: 'Áo', limit: 10 });

    expect(accented.map((r) => r.text)).toEqual(unaccented.map((r) => r.text));
    expect(accented.map((r) => r.type)).toEqual(unaccented.map((r) => r.type));
  });

  it('AC-I-03: uppercase prefix AO returns case-folded matches identically', async () => {
    const unaccented = await service.suggest({ prefix: 'ao', limit: 10 });
    const uppercase = await service.suggest({ prefix: 'AO', limit: 10 });

    expect(uppercase.map((r) => r.text)).toEqual(unaccented.map((r) => r.text));
  });

  it('AC-I-04: filters suggestions by category_id when provided', async () => {
    const results = await service.suggest({
      prefix: 'ao',
      limit: 10,
      category_id: 'category-outerwear',
    });

    const productSuggestions = results.filter(
      (r) => r.type === SuggestionType.PRODUCT,
    );
    expect(productSuggestions.length).toBeGreaterThan(0);
    for (const p of productSuggestions) {
      expect([
        'product-outerwear-long',
        'product-01912f31',
        'product-01912f32',
      ]).toContain(p.product_id);
    }
  });

  it('AC-I-05: returns empty array when category_id has no matching products', async () => {
    const results = await service.suggest({
      prefix: 'ao',
      limit: 10,
      category_id: 'category-phone',
    });

    expect(results).toEqual([]);
  });

  it('AC-I-06: returns BRAND suggestion alongside PRODUCT and QUERY suggestions for prefix apple', async () => {
    const results = await service.suggest({ prefix: 'apple', limit: 10 });

    const brandSuggestions = results.filter(
      (r) => r.type === SuggestionType.BRAND,
    );
    expect(brandSuggestions.length).toBeGreaterThan(0);
    expect(brandSuggestions.some((b) => b.text.toLowerCase() === 'apple')).toBe(
      true,
    );

    const productSuggestions = results.filter(
      (r) => r.type === SuggestionType.PRODUCT,
    );
    expect(productSuggestions.length).toBeGreaterThan(0);
  });

  it('AC-I-07: defaults total returned items to limit 10 when index contains >10 matches', async () => {
    const results = await service.suggest({ prefix: 'a' });

    expect(results.length).toBe(10);
  });

  it('AC-I-08: respects custom limit parameter (e.g. limit=3)', async () => {
    const results = await service.suggest({ prefix: 'a', limit: 3 });

    expect(results.length).toBe(3);
  });

  it('AC-I-09: returns empty array for non-existent prefix (does not throw 404)', async () => {
    const results = await service.suggest({
      prefix: 'xyznonexistent',
      limit: 10,
    });

    expect(results).toEqual([]);
  });

  it('AC-I-10: immediately excludes product when visibility_status becomes HIDDEN in index', async () => {
    // Initially product-outerwear-long is PUBLISHED
    const beforeResults = await service.suggest({ prefix: 'ao', limit: 10 });
    const beforeProdIds = beforeResults
      .filter((r) => r.type === SuggestionType.PRODUCT)
      .map((p) => p.product_id);
    expect(beforeProdIds).toContain('product-outerwear-long');

    // Simulate product unpublished by updating status to HIDDEN
    await client.update({
      index: concreteIndex,
      id: 'product-outerwear-long',
      doc: {
        visibility_status: 'HIDDEN',
      },
    });
    await client.indices.refresh({ index: concreteIndex });

    // Subsequent suggest call must exclude product-outerwear-long
    const afterResults = await service.suggest({ prefix: 'ao', limit: 10 });
    const afterProdIds = afterResults
      .filter((r) => r.type === SuggestionType.PRODUCT)
      .map((p) => p.product_id);
    expect(afterProdIds).not.toContain('product-outerwear-long');

    // Restore to PUBLISHED
    await client.update({
      index: concreteIndex,
      id: 'product-outerwear-long',
      doc: {
        visibility_status: 'PUBLISHED',
      },
    });
    await client.indices.refresh({ index: concreteIndex });
  });
});
