import { DomainEvent } from '../../event-consumer/interfaces';
import { ProductSearchDocument } from '../interfaces';
import { VariantAggregator } from './variant-aggregator';

describe('VariantAggregator', () => {
  let aggregator: VariantAggregator;

  beforeEach(() => {
    aggregator = new VariantAggregator();
  });

  const baseProduct: ProductSearchDocument = {
    product_id: 'prod-10',
    shop_id: 'shop-1',
    slug: 'ao-polo',
    title: 'Áo Polo',
    description: 'Mô tả',
    category_ids: ['cat-1'],
    category_path: ['ao'],
    brand: 'Taca',
    price: 200000,
    currency: 'VND',
    rating_avg: 0,
    rating_count: 0,
    attributes: {},
    variant_skus: {},
    visibility_status: 'PUBLISHED' as any,
    source_version: 1,
    source_event_id: 'evt-1',
    indexed_at: '2026-10-14T10:00:00.000Z',
  };

  it('aggregates attributes and attribute_pairs from active SKUs', () => {
    const sku1Event: DomainEvent = {
      event_id: 'sku-evt-1',
      schema_version: 1,
      event_type: 'sku.created',
      occurred_at: '2026-10-14T10:00:00.000Z',
      aggregate_type: 'SKU',
      aggregate_id: 'sku-1',
      version: 1,
      payload: {
        sku_id: 'sku-1',
        product_id: 'prod-10',
        status: 'ACTIVE',
        attributes: {
          color: 'Đỏ',
          size: 'M',
        },
      },
    };

    const res1 = aggregator.handleSkuEvent(baseProduct, sku1Event);
    expect(res1).not.toBeNull();
    expect(res1?.attributes).toEqual({
      color: ['Đỏ'],
      size: ['M'],
    });
    expect(res1?.attribute_pairs).toEqual(['color=Đỏ', 'size=M']);
    expect(res1?.variant_skus['sku-1']).toEqual({
      status: 'ACTIVE',
      attributes: { color: 'Đỏ', size: 'M' },
      version: 1,
    });

    // Add second SKU with overlapping color and new size
    const updatedProduct: ProductSearchDocument = {
      ...baseProduct,
      variant_skus: res1!.variant_skus,
      attributes: res1!.attributes,
      attribute_pairs: res1!.attribute_pairs,
    };

    const sku2Event: DomainEvent = {
      event_id: 'sku-evt-2',
      schema_version: 1,
      event_type: 'sku.created',
      occurred_at: '2026-10-14T10:01:00.000Z',
      aggregate_type: 'SKU',
      aggregate_id: 'sku-2',
      version: 1,
      payload: {
        sku_id: 'sku-2',
        product_id: 'prod-10',
        status: 'ACTIVE',
        attributes: {
          color: 'Xanh',
          size: 'M',
        },
      },
    };

    const res2 = aggregator.handleSkuEvent(updatedProduct, sku2Event);
    expect(res2?.attributes).toEqual({
      color: ['Xanh', 'Đỏ'],
      size: ['M'],
    });
    expect(res2?.attribute_pairs).toEqual(['color=Xanh', 'color=Đỏ', 'size=M']);
  });

  it('skips SKU event if version is older or equal to stored SKU version', () => {
    const productWithSku: ProductSearchDocument = {
      ...baseProduct,
      variant_skus: {
        'sku-1': {
          status: 'ACTIVE',
          attributes: { color: 'Đỏ' },
          version: 5,
        },
      },
    };

    const olderSkuEvent: DomainEvent = {
      event_id: 'sku-evt-old',
      schema_version: 1,
      event_type: 'sku.updated',
      occurred_at: '2026-10-14T10:00:00.000Z',
      aggregate_type: 'SKU',
      aggregate_id: 'sku-1',
      version: 4,
      payload: {
        sku_id: 'sku-1',
        product_id: 'prod-10',
        status: 'ACTIVE',
        attributes: { color: 'Vàng' },
      },
    };

    const res = aggregator.handleSkuEvent(productWithSku, olderSkuEvent);
    expect(res).toBeNull();
  });

  it('removes attribute value when SKU becomes INACTIVE unless another active SKU has it', () => {
    const productWithTwoSkus: ProductSearchDocument = {
      ...baseProduct,
      variant_skus: {
        'sku-1': {
          status: 'ACTIVE',
          attributes: { color: 'Đỏ', size: 'M' },
          version: 1,
        },
        'sku-2': {
          status: 'ACTIVE',
          attributes: { color: 'Xanh', size: 'M' },
          version: 1,
        },
      },
      attributes: {
        color: ['Xanh', 'Đỏ'],
        size: ['M'],
      },
      attribute_pairs: ['color=Xanh', 'color=Đỏ', 'size=M'],
    };

    // sku-1 becomes INACTIVE
    const sku1Inactive: DomainEvent = {
      event_id: 'sku-evt-3',
      schema_version: 1,
      event_type: 'sku.status_changed',
      occurred_at: '2026-10-14T10:05:00.000Z',
      aggregate_type: 'SKU',
      aggregate_id: 'sku-1',
      version: 2,
      payload: {
        sku_id: 'sku-1',
        product_id: 'prod-10',
        status: 'INACTIVE',
      },
    };

    const res = aggregator.handleSkuEvent(productWithTwoSkus, sku1Inactive);
    expect(res).not.toBeNull();
    // 'Đỏ' was only in sku-1, so it is gone. 'size: M' is still present in active sku-2!
    expect(res?.attributes).toEqual({
      color: ['Xanh'],
      size: ['M'],
    });
    expect(res?.attribute_pairs).toEqual(['color=Xanh', 'size=M']);
  });

  it('drops invalid attribute keys not matching regex /^[a-z0-9_]{1,64}$/', () => {
    const skuEventInvalidKey: DomainEvent = {
      event_id: 'sku-evt-4',
      schema_version: 1,
      event_type: 'sku.created',
      occurred_at: '2026-10-14T10:00:00.000Z',
      aggregate_type: 'SKU',
      aggregate_id: 'sku-1',
      version: 1,
      payload: {
        sku_id: 'sku-1',
        product_id: 'prod-10',
        status: 'ACTIVE',
        attributes: {
          'INVALID-KEY!': 'bad',
          valid_key: 'good',
        },
      },
    };

    const res = aggregator.handleSkuEvent(baseProduct, skuEventInvalidKey);
    expect(res?.attributes).toEqual({
      valid_key: ['good'],
    });
    expect(res?.attribute_pairs).toEqual(['valid_key=good']);
  });
});
