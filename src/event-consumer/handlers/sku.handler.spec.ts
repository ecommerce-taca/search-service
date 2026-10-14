import { Client } from '@elastic/elasticsearch';
import { SearchException } from '../../common/errors';
import { AppConfigService } from '../../config/config.service';
import { IIndexWriter } from '../../product-index/interfaces';
import { VariantAggregator } from '../../product-index/sku/variant-aggregator';
import { DomainEvent } from '../interfaces';
import { SkuHandler } from './sku.handler';

describe('SkuHandler.handleSkusUpdated (catalog product.skus_updated)', () => {
  let handler: SkuHandler;
  let get: jest.Mock;
  let writer: jest.Mocked<IIndexWriter>;

  const event = (payload: unknown, version = 3): DomainEvent<unknown> => ({
    event_id: 'evt-skus-1',
    schema_version: 1,
    event_type: 'product.skus_updated',
    occurred_at: 1760436000000,
    aggregate_type: 'PRODUCT',
    aggregate_id: 'prod-1',
    version,
    payload,
  });

  beforeEach(() => {
    get = jest.fn();
    writer = {
      bulkUpsert: jest.fn().mockResolvedValue(undefined),
      bulkPartialUpdate: jest.fn().mockResolvedValue(undefined),
      getCurrentVersions: jest.fn().mockResolvedValue(new Map()),
    };
    handler = new SkuHandler(
      { get } as unknown as Client,
      { esIndexPrefix: '' } as AppConfigService,
      new VariantAggregator(),
      writer,
    );
  });

  it('writes variant state replaced by the snapshot', async () => {
    get.mockResolvedValue({
      found: true,
      _source: { product_id: 'prod-1', variant_skus: {} },
    });

    await handler.handleSkusUpdated(
      event({
        product_id: 'prod-1',
        skus: [
          { sku_id: 'sku-1', status: 'ACTIVE', attributes: { color: 'Đen' } },
        ],
      }),
    );

    expect(writer.bulkPartialUpdate).toHaveBeenCalledTimes(1);
    const [, updates] = writer.bulkPartialUpdate.mock.calls[0];
    expect(updates[0].id).toBe('prod-1');
    expect(updates[0].doc.attributes).toEqual({ color: ['Đen'] });
    expect(updates[0].doc.attribute_pairs).toEqual(['color=Đen']);
  });

  it('does nothing when the product is not indexed yet', async () => {
    get.mockRejectedValue(
      Object.assign(new Error('not found'), { statusCode: 404 }),
    );
    get.mockResolvedValue({ found: false });

    await handler.handleSkusUpdated(event({ product_id: 'prod-1', skus: [] }));
    expect(writer.bulkPartialUpdate).not.toHaveBeenCalled();
  });

  it('rejects a snapshot without a skus array', async () => {
    await expect(
      handler.handleSkusUpdated(event({ product_id: 'prod-1' })),
    ).rejects.toBeInstanceOf(SearchException);
  });
});
