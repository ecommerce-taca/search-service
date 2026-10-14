import { Client } from '@elastic/elasticsearch';
import { Test, TestingModule } from '@nestjs/testing';
import * as fs from 'fs';
import * as path from 'path';
import {
  ES_REQUEST_TIMEOUT_MS,
  formatUtcTimestamp,
  getProductsAlias,
  getSearchCategoriesIndex,
  getVersionedIndexName,
} from '../src/common/constants';
import { SortOption } from '../src/common/enums';
import { AppConfigService } from '../src/config/config.service';
import { EsCategoryProjection } from '../src/product-index/category-projection.service';
import { ELASTICSEARCH_CLIENT } from '../src/product-index/elasticsearch.module';
import { CATEGORY_PROJECTION } from '../src/product-index/interfaces';
import {
  PRODUCTS_INDEX_MAPPING_V1,
  PRODUCTS_INDEX_SETTINGS_V1,
  SEARCH_CATEGORIES_MAPPING_V1,
} from '../src/product-index/mappings';
import { SearchProductsQueryDto } from '../src/search-query/dto/search-products.query';
import { SearchRequestBuilder } from '../src/search-query/es/search-request.builder';
import { QueryParser } from '../src/search-query/query-parser';
import { SearchResultMapper } from '../src/search-query/result.mapper';
import { SearchProductsService } from '../src/search-query/search-products.service';

describe('SearchQuery (Integration on real ES cluster)', () => {
  let client: Client;
  let service: SearchProductsService;
  let parser: QueryParser;
  let moduleRef: TestingModule;

  const rand = Math.random().toString(36).substring(2, 8);
  const testPrefix = process.env.ES_INDEX_PREFIX
    ? `${process.env.ES_INDEX_PREFIX}${rand}-`
    : `it-search-query-${rand}-`;

  const productsIndex = getVersionedIndexName(
    1,
    formatUtcTimestamp(),
    testPrefix,
  );
  const productsAlias = getProductsAlias(testPrefix);
  const categoriesIndex = getSearchCategoriesIndex(testPrefix);

  const esNode =
    process.env.ES_NODE && process.env.ES_NODE !== 'http://127.0.0.1:9200'
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
    kafkaGroupId: `search-service-group-${rand}`,
    kafkaTopicPrefix: `it-search-query-${rand}-`,
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
            return new Client({
              node: mockConfigService.esNode,
              requestTimeout: ES_REQUEST_TIMEOUT_MS,
            });
          },
        },
        {
          provide: Client,
          useExisting: ELASTICSEARCH_CLIENT,
        },
        {
          provide: CATEGORY_PROJECTION,
          useClass: EsCategoryProjection,
        },
        EsCategoryProjection,
        SearchRequestBuilder,
        SearchResultMapper,
        QueryParser,
        SearchProductsService,
      ],
    }).compile();

    client = moduleRef.get(Client);
    service = moduleRef.get(SearchProductsService);
    parser = moduleRef.get(QueryParser);

    // 1. Create categories index
    await client.indices.create({
      index: categoriesIndex,
      mappings: SEARCH_CATEGORIES_MAPPING_V1,
    });

    // 2. Create products versioned index
    await client.indices.create({
      index: productsIndex,
      settings: PRODUCTS_INDEX_SETTINGS_V1,
      mappings: PRODUCTS_INDEX_MAPPING_V1,
    });

    // 3. Put alias pointing to products index
    await client.indices.putAlias({
      index: productsIndex,
      name: productsAlias,
    });

    // 4. Seed fixtures from test/fixtures/search-query/search-query-fixtures.json
    const fixturePath = path.resolve(
      __dirname,
      'fixtures/search-query/search-query-fixtures.json',
    );
    const fixtureData = JSON.parse(fs.readFileSync(fixturePath, 'utf-8'));

    // Seed categories
    for (const cat of fixtureData.search_categories) {
      await client.index({
        index: categoriesIndex,
        id: cat.category_id,
        document: cat,
      });
    }

    // Seed products
    for (const prod of fixtureData.products) {
      const doc: Record<string, unknown> = {
        ...prod,
        shop: prod.shop || {
          shop_id: prod.shop_id,
          name: prod.shop_name,
        },
        created_at: prod.created_at || prod.indexed_at,
      };
      delete doc.shop_name;

      await client.index({
        index: productsIndex,
        id: prod.product_id,
        document: doc,
      });
    }

    // Refresh both indices
    await client.indices.refresh({ index: categoriesIndex });
    await client.indices.refresh({ index: productsIndex });
  });

  afterAll(async () => {
    try {
      if (await client.indices.existsAlias({ name: productsAlias })) {
        await client.indices.deleteAlias({
          index: productsIndex,
          name: productsAlias,
        });
      }
      if (await client.indices.exists({ index: productsIndex })) {
        await client.indices.delete({ index: productsIndex });
      }
      if (await client.indices.exists({ index: categoriesIndex })) {
        await client.indices.delete({ index: categoriesIndex });
      }
    } catch (cleanupErr) {
      console.warn(
        'Error during ES cleanup in search-query.int-spec:',
        cleanupErr,
      );
    }

    await client.close();
    await moduleRef.close();
  });

  it('SQ-I-01: matches unaccented Vietnamese "ao khoac" returning prod-001 and prod-002 via vi folding', async () => {
    const res = await service.search(
      parser.parse({ q: 'ao khoac' } as unknown as SearchProductsQueryDto),
    );
    expect(res.meta.total).toBe(2);
    const ids = res.data.map((p) => p.product_id);
    expect(ids).toContain('prod-001');
    expect(ids).toContain('prod-002');
  });

  it('SQ-I-02: matches accented Vietnamese "Áo khoác" returning prod-001 and prod-002', async () => {
    const res = await service.search(
      parser.parse({ q: 'Áo khoác' } as unknown as SearchProductsQueryDto),
    );
    expect(res.meta.total).toBe(2);
    const ids = res.data.map((p) => p.product_id);
    expect(ids).toContain('prod-001');
    expect(ids).toContain('prod-002');
  });

  it('SQ-I-03: matches uppercase unaccented "AO KHOAC" returning prod-001 and prod-002', async () => {
    const res = await service.search(
      parser.parse({ q: 'AO KHOAC' } as unknown as SearchProductsQueryDto),
    );
    expect(res.meta.total).toBe(2);
    const ids = res.data.map((p) => p.product_id);
    expect(ids).toContain('prod-001');
    expect(ids).toContain('prod-002');
  });

  it('SQ-I-04: matches unaccented "dien thoai" returning 4 phones (prod-003..006)', async () => {
    const res = await service.search(
      parser.parse({ q: 'dien thoai' } as unknown as SearchProductsQueryDto),
    );
    expect(res.meta.total).toBe(4);
    const ids = res.data.map((p) => p.product_id);
    expect(ids).toEqual(
      expect.arrayContaining(['prod-003', 'prod-004', 'prod-005', 'prod-006']),
    );
  });

  it('SQ-I-05: NEVER returns HIDDEN product prod-009 when searching "da lon" or "Áo khoác da lộn"', async () => {
    const res1 = await service.search(
      parser.parse({ q: 'da lon' } as unknown as SearchProductsQueryDto),
    );
    expect(res1.meta.total).toBe(0);
    expect(res1.data).toHaveLength(0);

    const res2 = await service.search(
      parser.parse({
        q: 'Áo khoác da lộn',
      } as unknown as SearchProductsQueryDto),
    );
    expect(res2.meta.total).toBe(0);
    expect(res2.data).toHaveLength(0);
  });

  it('SQ-I-06: NEVER returns DELETED product prod-010 when searching "iPhone 14 Pro"', async () => {
    const res = await service.search(
      parser.parse({ q: 'iPhone 14 Pro' } as unknown as SearchProductsQueryDto),
    );
    expect(res.meta.total).toBe(0);
    expect(res.data).toHaveLength(0);
  });

  it('SQ-I-07: strictly excludes HIDDEN and DELETED docs from all facet aggregations', async () => {
    const res = await service.search(
      parser.parse({} as unknown as SearchProductsQueryDto),
    );
    const facets = res.meta.facets!;

    // Apple count = 3 (prod-003, 004, 007; NOT prod-010 which is DELETED)
    const appleBrand = facets.brand.find((b) => b.value === 'Apple');
    expect(appleBrand?.count).toBe(3);

    // Taca Brand count = 1 (prod-001; NOT prod-009 which is HIDDEN)
    const tacaBrand = facets.brand.find((b) => b.value === 'Taca Brand');
    expect(tacaBrand?.count).toBe(1);

    // Price bucket [0, 5000000) count = 3 (prod-001, 002, 006; NOT prod-009)
    const lowBucket = facets.price.find(
      (b) => b.min === 0 && b.max === 5000000,
    );
    expect(lowBucket?.count).toBe(3);
  });

  it('SQ-I-08: returns empty array data=[] and total=0 for non-existent query (not 404)', async () => {
    const res = await service.search(
      parser.parse({
        q: 'khongtontaicainay123456789',
      } as unknown as SearchProductsQueryDto),
    );
    expect(res.data).toEqual([]);
    expect(res.meta.total).toBe(0);
    expect(res.meta.total_pages).toBe(0);
  });

  it('SQ-I-09: returns all 8 PUBLISHED products in browse mode without q', async () => {
    const res = await service.search(
      parser.parse({} as unknown as SearchProductsQueryDto),
    );
    expect(res.meta.total).toBe(8);
    expect(res.meta.total_pages).toBe(1);
    expect(res.data).toHaveLength(8);
  });

  it('SQ-I-10: filters by category_id=cat-outerwear returning prod-001 and prod-002', async () => {
    const res = await service.search(
      parser.parse({
        category_id: 'cat-outerwear',
      } as unknown as SearchProductsQueryDto),
    );
    expect(res.meta.total).toBe(2);
    const ids = res.data.map((p) => p.product_id);
    expect(ids).toEqual(expect.arrayContaining(['prod-001', 'prod-002']));
  });

  it('SQ-I-11: filters by shop_id=shop-001 returning prod-001 and prod-002', async () => {
    const res = await service.search(
      parser.parse({
        shop_id: 'shop-001',
      } as unknown as SearchProductsQueryDto),
    );
    expect(res.meta.total).toBe(2);
    const ids = res.data.map((p) => p.product_id);
    expect(ids).toEqual(expect.arrayContaining(['prod-001', 'prod-002']));
  });

  it('SQ-I-12: sorts by relevance with q=iPhone&sort=relevance and deterministic product_id asc tie-break', async () => {
    const res = await service.search(
      parser.parse({
        q: 'iPhone',
        sort: SortOption.RELEVANCE,
      } as unknown as SearchProductsQueryDto),
    );
    expect(res.meta.total).toBeGreaterThan(0);
    // All returned items match iPhone
    for (const item of res.data) {
      expect(item.title.toLowerCase()).toContain('iphone');
    }
  });

  it('SQ-I-13: sorts by newest in descending order with product_id asc tie-break per D6', async () => {
    const res = await service.search(
      parser.parse({
        sort: SortOption.NEWEST,
        size: 10,
      } as unknown as SearchProductsQueryDto),
    );
    const ids = res.data.map((p) => p.product_id);
    expect(ids).toEqual([
      'prod-003',
      'prod-007',
      'prod-002',
      'prod-001',
      'prod-006',
      'prod-005',
      'prod-004',
      'prod-008',
    ]);
  });

  it('SQ-I-14: sorts by price_asc in ascending price order with product_id asc tie-break', async () => {
    const res = await service.search(
      parser.parse({
        sort: SortOption.PRICE_ASC,
        size: 10,
      } as unknown as SearchProductsQueryDto),
    );
    const prices = res.data.map((p) => p.price.amount);
    expect(prices).toEqual([
      350000, 450000, 4490000, 14500000, 19990000, 27990000, 29990000, 34990000,
    ]);
    const ids = res.data.map((p) => p.product_id);
    expect(ids).toEqual([
      'prod-001',
      'prod-002',
      'prod-006',
      'prod-008',
      'prod-004',
      'prod-007',
      'prod-005',
      'prod-003',
    ]);
  });

  it('SQ-I-15: sorts by price_desc in descending price order', async () => {
    const res = await service.search(
      parser.parse({
        sort: SortOption.PRICE_DESC,
        size: 10,
      } as unknown as SearchProductsQueryDto),
    );
    const prices = res.data.map((p) => p.price.amount);
    expect(prices).toEqual([
      34990000, 29990000, 27990000, 19990000, 14500000, 4490000, 450000, 350000,
    ]);
    const ids = res.data.map((p) => p.product_id);
    expect(ids).toEqual([
      'prod-003',
      'prod-005',
      'prod-007',
      'prod-004',
      'prod-008',
      'prod-006',
      'prod-002',
      'prod-001',
    ]);
  });

  it('SQ-I-16: sorts by rating_desc with product_id tie-break when ratings collide', async () => {
    const res = await service.search(
      parser.parse({
        sort: SortOption.RATING_DESC,
        size: 10,
      } as unknown as SearchProductsQueryDto),
    );
    const ids = res.data.map((p) => p.product_id);

    // Both prod-003 and prod-007 have rating 4.9; tie-break by product_id asc puts prod-003 first
    expect(ids[0]).toBe('prod-003');
    expect(ids[1]).toBe('prod-007');
    expect(ids).toEqual([
      'prod-003',
      'prod-007',
      'prod-001',
      'prod-004',
      'prod-005',
      'prod-008',
      'prod-002',
      'prod-006',
    ]);
  });

  it('SQ-I-17: paginates page 1 of 3: page=1&size=3&sort=price_asc', async () => {
    const res = await service.search(
      parser.parse({
        page: 1,
        size: 3,
        sort: SortOption.PRICE_ASC,
      } as unknown as SearchProductsQueryDto),
    );
    expect(res.meta).toMatchObject({
      page: 1,
      size: 3,
      total: 8,
      total_pages: 3,
    });
    expect(res.data.map((p) => p.product_id)).toEqual([
      'prod-001',
      'prod-002',
      'prod-006',
    ]);
  });

  it('SQ-I-18: paginates page 2 of 3: page=2&size=3&sort=price_asc', async () => {
    const res = await service.search(
      parser.parse({
        page: 2,
        size: 3,
        sort: SortOption.PRICE_ASC,
      } as unknown as SearchProductsQueryDto),
    );
    expect(res.meta).toMatchObject({
      page: 2,
      size: 3,
      total: 8,
      total_pages: 3,
    });
    expect(res.data.map((p) => p.product_id)).toEqual([
      'prod-008',
      'prod-004',
      'prod-007',
    ]);
  });

  it('SQ-I-19: paginates page 3 of 3: page=3&size=3&sort=price_asc', async () => {
    const res = await service.search(
      parser.parse({
        page: 3,
        size: 3,
        sort: SortOption.PRICE_ASC,
      } as unknown as SearchProductsQueryDto),
    );
    expect(res.meta).toMatchObject({
      page: 3,
      size: 3,
      total: 8,
      total_pages: 3,
    });
    expect(res.data.map((p) => p.product_id)).toEqual(['prod-005', 'prod-003']);
  });

  it('SQ-I-20: paginates beyond total pages page=5&size=3 returning data=[]', async () => {
    const res = await service.search(
      parser.parse({
        page: 5,
        size: 3,
        sort: SortOption.PRICE_ASC,
      } as unknown as SearchProductsQueryDto),
    );
    expect(res.data).toEqual([]);
    expect(res.meta).toMatchObject({
      page: 5,
      size: 3,
      total: 8,
      total_pages: 3,
    });
  });

  it('SQ-I-21: resolves category facet names and direct-children scope per D4', async () => {
    // Browse mode without category filter: returns root categories (cat-tech, cat-fashion)
    const browseRes = await service.search(
      parser.parse({} as unknown as SearchProductsQueryDto),
    );
    const catFacets = browseRes.meta.facets!.category;
    expect(catFacets).toEqual([
      { category_id: 'cat-tech', name: 'Đồ công nghệ', count: 6 },
      { category_id: 'cat-fashion', name: 'Thời trang', count: 2 },
    ]);

    // Filtered by category_id=cat-tech: returns direct children (cat-phone, cat-laptop)
    const techRes = await service.search(
      parser.parse({
        category_id: 'cat-tech',
      } as unknown as SearchProductsQueryDto),
    );
    expect(techRes.meta.facets!.category).toEqual([
      { category_id: 'cat-phone', name: 'Điện thoại', count: 4 },
      { category_id: 'cat-laptop', name: 'Laptop', count: 2 },
    ]);
  });

  it('SQ-I-22: aggregates brand facet counts accurately', async () => {
    const res = await service.search(
      parser.parse({} as unknown as SearchProductsQueryDto),
    );
    const brandFacets = res.meta.facets!.brand;
    expect(brandFacets).toEqual([
      { value: 'Apple', count: 3 },
      { value: 'Samsung', count: 2 },
      { value: 'Ananas', count: 1 },
      { value: 'Asus', count: 1 },
      { value: 'Taca Brand', count: 1 },
    ]);
  });

  it('SQ-I-23: aggregates price facet with 3 fixed buckets per D9', async () => {
    const res = await service.search(
      parser.parse({} as unknown as SearchProductsQueryDto),
    );
    const priceFacets = res.meta.facets!.price;
    expect(priceFacets).toEqual([
      { min: 0, max: 5000000, count: 3 },
      { min: 5000000, max: 20000000, count: 2 },
      { min: 20000000, max: null, count: 3 },
    ]);
  });

  it('SQ-I-24: aggregates dynamic variant dimension attributes (color, capacity_liter, size)', async () => {
    const res = await service.search(
      parser.parse({} as unknown as SearchProductsQueryDto),
    );
    const attrFacets = res.meta.facets!.attributes;

    expect(attrFacets.capacity_liter).toBeDefined();
    const capacityCounts = new Map(
      attrFacets.capacity_liter.map((i) => [i.value, i.count]),
    );
    expect(capacityCounts.get('256GB')).toBe(3);
    expect(capacityCounts.get('512GB')).toBe(3);
    expect(capacityCounts.get('128GB')).toBe(2);

    expect(attrFacets.color).toBeDefined();
    expect(attrFacets.size).toBeDefined();
  });

  it('SQ-I-25: strictly excludes spec_attributes from dynamic attribute facets', async () => {
    const res = await service.search(
      parser.parse({} as unknown as SearchProductsQueryDto),
    );
    const attrFacets = res.meta.facets!.attributes;
    const keys = Object.keys(attrFacets);

    // None of spec_attributes should appear
    for (const specKey of [
      'material',
      'screen_size',
      'chip',
      'battery',
      'ram',
      'cpu',
      'condition',
      'origin',
      'stylus',
    ]) {
      expect(keys).not.toContain(specKey);
    }
  });

  it('SQ-I-26: applies multi-select post_filter on brand so brand facet counts are retained', async () => {
    const res = await service.search(
      parser.parse({ brand: ['Apple'] } as unknown as SearchProductsQueryDto),
    );

    // Data contains only 3 Apple products
    expect(res.meta.total).toBe(3);
    for (const p of res.data) {
      expect(['prod-003', 'prod-004', 'prod-007']).toContain(p.product_id);
    }

    // Crucial: meta.facets.brand continues to return all brands (Samsung 2, Asus 1, Taca Brand 1, Ananas 1, Apple 3)
    const brandFacets = res.meta.facets!.brand;
    const brandMap = new Map(brandFacets.map((b) => [b.value, b.count]));
    expect(brandMap.get('Apple')).toBe(3);
    expect(brandMap.get('Samsung')).toBe(2);
    expect(brandMap.get('Asus')).toBe(1);

    // Concurrently, category facet reflects only categories containing Apple
    const catFacets = res.meta.facets!.category;
    expect(
      catFacets.find((c) => c.category_id === 'cat-fashion'),
    ).toBeUndefined();
    expect(catFacets.find((c) => c.category_id === 'cat-tech')?.count).toBe(3);
  });

  it('SQ-I-27: applies multi-select post_filter on category retaining category counts while filtering brand', async () => {
    const res = await service.search(
      parser.parse({
        category_id: 'cat-outerwear',
      } as unknown as SearchProductsQueryDto),
    );
    expect(res.meta.total).toBe(2);
    const ids = res.data.map((p) => p.product_id);
    expect(ids).toEqual(expect.arrayContaining(['prod-001', 'prod-002']));

    // Brand facet filters down to Taca Brand and Ananas
    const brandFacets = res.meta.facets!.brand;
    const brandMap = new Map(brandFacets.map((b) => [b.value, b.count]));
    expect(brandMap.get('Taca Brand')).toBe(1);
    expect(brandMap.get('Ananas')).toBe(1);
    expect(brandMap.get('Apple')).toBeUndefined();
  });

  it('SQ-I-28: applies multi-select post_filter on attribute so other values in same key are retained', async () => {
    const res = await service.search(
      parser.parse({
        attribute: { capacity_liter: '256GB' },
      } as unknown as SearchProductsQueryDto),
    );
    // Data contains prod-003, prod-005, prod-007
    expect(res.meta.total).toBe(3);
    const ids = res.data.map((p) => p.product_id);
    expect(ids).toEqual(
      expect.arrayContaining(['prod-003', 'prod-005', 'prod-007']),
    );

    // capacity_liter facet still reports counts for 128GB and 512GB (allowing multi-select)
    const capFacet = res.meta.facets!.attributes.capacity_liter;
    const capMap = new Map(capFacet.map((c) => [c.value, c.count]));
    expect(capMap.get('256GB')).toBe(3);
    expect(capMap.get('512GB')).toBe(3);
    expect(capMap.get('128GB')).toBe(2);

    // brand facet shows Apple (2) and Samsung (1)
    const brandMap = new Map(
      res.meta.facets!.brand.map((b) => [b.value, b.count]),
    );
    expect(brandMap.get('Apple')).toBe(2);
    expect(brandMap.get('Samsung')).toBe(1);
  });

  it('SQ-I-29: combines category, brand, and price filters returning exact match prod-005', async () => {
    const res = await service.search(
      parser.parse({
        category_id: 'cat-tech',
        brand: ['Samsung'],
        min_price: 20000000,
      } as unknown as SearchProductsQueryDto),
    );
    expect(res.meta.total).toBe(1);
    expect(res.data[0].product_id).toBe('prod-005');
    expect(res.data[0].title).toBe('Điện thoại Samsung Galaxy S24 Ultra');
    expect(res.data[0].price.amount).toBe(29990000);
  });
});
