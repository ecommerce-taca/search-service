import { VariantAggregator } from './variant-aggregator';

describe('VariantAggregator', () => {
  let aggregator: VariantAggregator;

  beforeEach(() => {
    aggregator = new VariantAggregator();
  });

  describe('[IDX-SKU-01] adding new active SKU', () => {
    it('[IDX-SKU-01] should store SKU in variant_skus and aggregate its attributes', () => {
      const existingDoc = {
        variant_skus: {},
        attributes: {},
        attribute_pairs: [],
      };

      const skuEvent = {
        sku_id: 'sku-1',
        product_id: 'prod-001',
        status: 'ACTIVE',
        attributes: { color: 'Đen', size: 'M' },
        version: 1,
      };

      const result = aggregator.aggregate(existingDoc, skuEvent);

      expect(result.variant_skus['sku-1']).toEqual({
        status: 'ACTIVE',
        attributes: { color: 'Đen', size: 'M' },
        version: 1,
      });

      expect(result.attributes).toEqual({
        color: ['Đen'],
        size: ['M'],
      });

      expect(result.attribute_pairs).toEqual(
        expect.arrayContaining(['color=Đen', 'size=M']),
      );
    });
  });

  describe('[IDX-SKU-02] merging multiple active SKUs', () => {
    it('[IDX-SKU-02] should union values, deduplicate and sort values alphabetically', () => {
      const existingDoc = {
        variant_skus: {
          'sku-1': {
            status: 'ACTIVE',
            attributes: { color: 'Đen', size: 'M' },
            version: 1,
          },
        },
        attributes: {
          color: ['Đen'],
          size: ['M'],
        },
        attribute_pairs: ['color=Đen', 'size=M'],
      };

      const sku2Event = {
        sku_id: 'sku-2',
        product_id: 'prod-001',
        status: 'ACTIVE',
        attributes: { color: 'Trắng', size: 'M' },
        version: 1,
      };

      const result = aggregator.aggregate(existingDoc, sku2Event);

      expect(result.variant_skus['sku-2']).toBeDefined();
      expect(result.attributes.color).toEqual(['Đen', 'Trắng']); // deduped & sorted
      expect(result.attributes.size).toEqual(['M']); // deduped 'M'
      expect(result.attribute_pairs).toEqual(
        expect.arrayContaining(['color=Đen', 'color=Trắng', 'size=M']),
      );
    });
  });

  describe('[IDX-SKU-03] SKU status change to INACTIVE', () => {
    it('[IDX-SKU-03] should recompute attributes and remove values not present in any other ACTIVE SKU', () => {
      const existingDoc = {
        variant_skus: {
          'sku-1': {
            status: 'ACTIVE',
            attributes: { color: 'Đen', size: 'M' },
            version: 1,
          },
          'sku-2': {
            status: 'ACTIVE',
            attributes: { color: 'Trắng', size: 'L' },
            version: 1,
          },
        },
        attributes: {
          color: ['Đen', 'Trắng'],
          size: ['L', 'M'],
        },
        attribute_pairs: ['color=Đen', 'color=Trắng', 'size=L', 'size=M'],
      };

      const deactivateSku2Event = {
        sku_id: 'sku-2',
        product_id: 'prod-001',
        status: 'INACTIVE',
        attributes: { color: 'Trắng', size: 'L' },
        version: 2,
      };

      const result = aggregator.aggregate(existingDoc, deactivateSku2Event);

      expect(result.variant_skus['sku-2'].status).toBe('INACTIVE');
      // Trắng and L should be removed because only sku-2 had them
      expect(result.attributes.color).toEqual(['Đen']);
      expect(result.attributes.size).toEqual(['M']);
      expect(result.attribute_pairs).toEqual(
        expect.arrayContaining(['color=Đen', 'size=M']),
      );
      expect(result.attribute_pairs).not.toContain('color=Trắng');
      expect(result.attribute_pairs).not.toContain('size=L');
    });

    it('[IDX-SKU-03] should keep attribute value when another ACTIVE SKU still has the same value', () => {
      const existingDoc = {
        variant_skus: {
          'sku-1': {
            status: 'ACTIVE',
            attributes: { color: 'Đen', size: 'M' },
            version: 1,
          },
          'sku-2': {
            status: 'ACTIVE',
            attributes: { color: 'Đen', size: 'L' },
            version: 1,
          },
        },
        attributes: {
          color: ['Đen'],
          size: ['L', 'M'],
        },
        attribute_pairs: ['color=Đen', 'size=L', 'size=M'],
      };

      const deactivateSku2Event = {
        sku_id: 'sku-2',
        product_id: 'prod-001',
        status: 'INACTIVE',
        attributes: { color: 'Đen', size: 'L' },
        version: 2,
      };

      const result = aggregator.aggregate(existingDoc, deactivateSku2Event);

      expect(result.variant_skus['sku-2'].status).toBe('INACTIVE');
      // color 'Đen' is still kept because sku-1 has color: 'Đen'
      expect(result.attributes.color).toEqual(['Đen']);
      expect(result.attributes.size).toEqual(['M']);
      expect(result.attribute_pairs).toContain('color=Đen');
      expect(result.attribute_pairs).not.toContain('size=L');
    });
  });

  describe('[IDX-SKU-04] ignoring out-of-order SKU events', () => {
    it('[IDX-SKU-04] should ignore SKU event if version is older than stored per-SKU version', () => {
      const existingDoc = {
        variant_skus: {
          'sku-1': {
            status: 'ACTIVE',
            attributes: { color: 'Đen' },
            version: 5,
          },
        },
        attributes: { color: ['Đen'] },
        attribute_pairs: ['color=Đen'],
      };

      const staleEvent = {
        sku_id: 'sku-1',
        product_id: 'prod-001',
        status: 'INACTIVE',
        attributes: { color: 'Đen' },
        version: 3, // older than 5
      };

      const result = aggregator.aggregate(existingDoc, staleEvent);

      // Should remain unchanged
      expect(result.variant_skus['sku-1'].version).toBe(5);
      expect(result.variant_skus['sku-1'].status).toBe('ACTIVE');
      expect(result.attributes.color).toEqual(['Đen']);
    });
  });

  describe('[IDX-SKU-05] generating attribute_pairs (D23)', () => {
    it('[IDX-SKU-05] should correctly generate <key>=<value> pairs for all aggregated attributes', () => {
      const existingDoc = {
        variant_skus: {},
        attributes: {},
        attribute_pairs: [],
      };

      const skuEvent = {
        sku_id: 'sku-1',
        product_id: 'prod-001',
        status: 'ACTIVE',
        attributes: {
          ram: '16GB',
          storage: '512GB',
        },
        version: 1,
      };

      const result = aggregator.aggregate(existingDoc, skuEvent);

      expect(result.attribute_pairs).toEqual(
        expect.arrayContaining(['ram=16GB', 'storage=512GB']),
      );
      expect(result.attribute_pairs.length).toBe(2);
    });
  });

  describe('[IDX-SKU-06] dropping invalid attribute keys', () => {
    it('[IDX-SKU-06] should drop keys not matching /^[a-z0-9_]{1,64}$/ and log warning without leaking data', () => {
      const existingDoc = {
        variant_skus: {},
        attributes: {},
        attribute_pairs: [],
      };

      const skuEvent = {
        sku_id: 'sku-invalid-keys',
        product_id: 'prod-001',
        status: 'ACTIVE',
        attributes: {
          valid_key_1: 'val1',
          'INVALID-UPPERCASE': 'bad1',
          'space key': 'bad2',
          'key!@#': 'bad3',
          '': 'bad4',
          'a'.repeat(65): 'too_long',
        },
        version: 1,
      };

      const result = aggregator.aggregate(existingDoc, skuEvent);

      expect(result.attributes.valid_key_1).toBeDefined();
      expect(result.attributes['INVALID-UPPERCASE']).toBeUndefined();
      expect(result.attributes['space key']).toBeUndefined();
      expect(result.attributes['key!@#']).toBeUndefined();
      expect(result.attributes['']).toBeUndefined();
      expect(result.attributes['a'.repeat(65)]).toBeUndefined();

      expect(result.attribute_pairs).toEqual(['valid_key_1=val1']);
    });
  });
});
