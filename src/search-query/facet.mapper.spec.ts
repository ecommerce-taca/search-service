import { estypes } from '@elastic/elasticsearch';
import {
  CategoryProjection,
  ICategoryProjection,
} from '../product-index/interfaces';
import { FacetMapper } from './facet.mapper';

describe('FacetMapper', () => {
  let mapper: FacetMapper;
  let mockCategoryProjection: jest.Mocked<ICategoryProjection>;

  const makeCategory = (
    category_id: string,
    name: string,
    parent_id: string | null = null,
  ): CategoryProjection => ({
    category_id,
    name,
    parent_id,
    path_ids: [category_id],
    status: 'ACTIVE',
    source_version: 1,
    updated_at: '2026-08-30T09:00:00Z',
  });

  beforeEach(() => {
    mockCategoryProjection = {
      getMany: jest.fn(),
      resolvePathIds: jest.fn(),
    } as unknown as jest.Mocked<ICategoryProjection>;
    mapper = new FacetMapper(mockCategoryProjection);
  });

  describe('buildCategoryFacet', () => {
    it('D4: resolves root categories when selectedCategoryId is undefined, dropping unknown and subcategories', async () => {
      mockCategoryProjection.getMany.mockResolvedValue(
        new Map<string, CategoryProjection>([
          ['cat-tech', makeCategory('cat-tech', 'Đồ công nghệ', null)],
          ['cat-fashion', makeCategory('cat-fashion', 'Thời trang', '')],
          ['cat-phone', makeCategory('cat-phone', 'Điện thoại', 'cat-tech')],
        ]),
      );

      const rawBuckets = [
        { key: 'cat-fashion', doc_count: 2 },
        { key: 'cat-tech', doc_count: 6 },
        { key: 'cat-phone', doc_count: 4 },
        { key: 'cat-unknown', doc_count: 5 },
        { key: 'cat-zero', doc_count: 0 },
      ];

      const result = await mapper.buildCategoryFacet(rawBuckets, undefined);

      expect(result).toEqual([
        { category_id: 'cat-tech', name: 'Đồ công nghệ', count: 6 },
        { category_id: 'cat-fashion', name: 'Thời trang', count: 2 },
      ]);
    });

    it('D4: resolves direct children when selectedCategoryId is specified', async () => {
      mockCategoryProjection.getMany.mockResolvedValue(
        new Map<string, CategoryProjection>([
          ['cat-phone', makeCategory('cat-phone', 'Điện thoại', 'cat-tech')],
          ['cat-laptop', makeCategory('cat-laptop', 'Laptop', 'cat-tech')],
          ['cat-case', makeCategory('cat-case', 'Ốp lưng', 'cat-phone')],
        ]),
      );

      const rawBuckets = [
        { key: 'cat-phone', doc_count: 4 },
        { key: 'cat-laptop', doc_count: 2 },
        { key: 'cat-case', doc_count: 1 },
      ];

      const result = await mapper.buildCategoryFacet(rawBuckets, 'cat-tech');

      expect(result).toEqual([
        { category_id: 'cat-phone', name: 'Điện thoại', count: 4 },
        { category_id: 'cat-laptop', name: 'Laptop', count: 2 },
      ]);
    });
  });

  describe('buildBrandFacet', () => {
    it('aggregates brand buckets sorted by count desc then name asc, dropping zero count', () => {
      const rawBuckets = [
        { key: 'Samsung', doc_count: 5 },
        { key: 'Apple', doc_count: 5 },
        { key: 'Xiaomi', doc_count: 2 },
        { key: 'Nokia', doc_count: 0 },
      ];

      const result = mapper.buildBrandFacet(rawBuckets);

      expect(result).toEqual([
        { value: 'Apple', count: 5 },
        { value: 'Samsung', count: 5 },
        { value: 'Xiaomi', count: 2 },
      ]);
    });
  });

  describe('buildPriceFacet', () => {
    it('D9: maps fixed price buckets by key, not by array index', () => {
      // Intentionally provide buckets out of order
      const rawBuckets = [
        { key: 'gte_20m', doc_count: 15 },
        { key: 'lt_5m', doc_count: 8 },
        { key: '5m_20m', doc_count: 12 },
      ];

      const result = mapper.buildPriceFacet(rawBuckets);

      expect(result).toEqual([
        { min: 0, max: 5000000, count: 8 },
        { min: 5000000, max: 20000000, count: 12 },
        { min: 20000000, max: null, count: 15 },
      ]);
    });

    it('defaults missing price buckets to count 0', () => {
      const rawBuckets = [{ key: 'lt_5m', doc_count: 3 }];

      const result = mapper.buildPriceFacet(rawBuckets);

      expect(result).toEqual([
        { min: 0, max: 5000000, count: 3 },
        { min: 5000000, max: 20000000, count: 0 },
        { min: 20000000, max: null, count: 0 },
      ]);
    });

    it('matches by from/to when key format varies', () => {
      const rawBuckets = [
        { from: 0, to: 5000000, doc_count: 5 },
        { from: 5000000, to: 20000000, doc_count: 10 },
        { from: 20000000, doc_count: 7 },
      ];

      const result = mapper.buildPriceFacet(rawBuckets);

      expect(result).toEqual([
        { min: 0, max: 5000000, count: 5 },
        { min: 5000000, max: 20000000, count: 10 },
        { min: 20000000, max: null, count: 7 },
      ]);
    });
  });

  describe('buildAttributeFacets', () => {
    it('D23: aggregates attribute facets from attr_sel_<k> and attr_rest keeping top 10 keys and <=30 values', () => {
      const aggs: Record<string, estypes.AggregationsAggregate> = {
        attr_sel_color: {
          values: {
            buckets: [
              { key: 'color=Đen', doc_count: 5 },
              { key: 'color=Trắng', doc_count: 2 },
            ],
          },
        } as unknown as estypes.AggregationsAggregate,
        attr_rest: {
          values: {
            buckets: [
              { key: 'capacity_liter=256GB', doc_count: 4 },
              { key: 'capacity_liter=512GB', doc_count: 3 },
              { key: 'size=L', doc_count: 2 },
            ],
          },
        } as unknown as estypes.AggregationsAggregate,
      };

      const result = mapper.buildAttributeFacets(aggs, ['color']);

      expect(result).toEqual({
        color: [
          { value: 'Đen', count: 5 },
          { value: 'Trắng', count: 2 },
        ],
        capacity_liter: [
          { value: '256GB', count: 4 },
          { value: '512GB', count: 3 },
        ],
        size: [{ value: 'L', count: 2 }],
      });
    });

    it('caps attribute keys at 10 and values at 30', () => {
      const restBuckets: Array<{ key: string; doc_count: number }> = [];
      // Generate 12 keys
      for (let k = 1; k <= 12; k++) {
        // For key_1, generate 35 values
        const valueCount = k === 1 ? 35 : 2;
        for (let v = 1; v <= valueCount; v++) {
          restBuckets.push({
            key: `attr_${k}=val_${v}`,
            doc_count: (13 - k) * 10 + (36 - v),
          });
        }
      }

      const aggs = {
        attr_rest: {
          values: { buckets: restBuckets },
        },
      } as unknown as Record<string, estypes.AggregationsAggregate>;

      const result = mapper.buildAttributeFacets(aggs, []);

      // Top 10 keys only
      expect(Object.keys(result)).toHaveLength(10);
      expect(result['attr_11']).toBeUndefined();
      expect(result['attr_12']).toBeUndefined();

      // Top 30 values only for attr_1
      expect(result['attr_1']).toHaveLength(30);
    });
  });

  describe('mapFacets', () => {
    it('orchestrates all facets correctly', async () => {
      mockCategoryProjection.getMany.mockResolvedValue(
        new Map<string, CategoryProjection>([
          ['cat-root', makeCategory('cat-root', 'Gốc', null)],
        ]),
      );

      const aggs: Record<string, estypes.AggregationsAggregate> = {
        category: {
          values: {
            buckets: [{ key: 'cat-root', doc_count: 10 }],
          },
        } as unknown as estypes.AggregationsAggregate,
        brand: {
          values: {
            buckets: [{ key: 'Apple', doc_count: 7 }],
          },
        } as unknown as estypes.AggregationsAggregate,
        price: {
          values: {
            buckets: [{ key: 'lt_5m', doc_count: 3 }],
          },
        } as unknown as estypes.AggregationsAggregate,
      };

      const result = await mapper.mapFacets(aggs, undefined, []);

      expect(result.category).toEqual([
        { category_id: 'cat-root', name: 'Gốc', count: 10 },
      ]);
      expect(result.brand).toEqual([{ value: 'Apple', count: 7 }]);
      expect(result.price[0]).toEqual({ min: 0, max: 5000000, count: 3 });
      expect(result.attributes).toEqual({});
    });
  });
});
