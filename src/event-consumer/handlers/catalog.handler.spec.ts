import { AppConfigService } from '../../config/config.service';
import {
  ICategoryProjection,
  IIndexWriter,
} from '../../product-index/interfaces';
import { DomainEvent } from '../interfaces';
import { CatalogHandler } from './catalog.handler';

describe('CatalogHandler product.category_changed', () => {
  let handler: CatalogHandler;
  let writer: jest.Mocked<IIndexWriter>;
  let projection: jest.Mocked<ICategoryProjection>;

  const event = (payload: unknown): DomainEvent<unknown> => ({
    event_id: 'evt-cat-1',
    schema_version: 1,
    event_type: 'product.category_changed',
    occurred_at: '2026-10-14T10:00:00Z',
    aggregate_type: 'PRODUCT',
    aggregate_id: 'prod-1',
    version: 4,
    payload,
  });

  beforeEach(() => {
    writer = {
      bulkUpsert: jest.fn().mockResolvedValue(undefined),
      bulkPartialUpdate: jest.fn().mockResolvedValue(undefined),
      getCurrentVersions: jest.fn().mockResolvedValue(new Map()),
      updateShopSnapshot: jest.fn().mockResolvedValue(0),
    };
    projection = {
      getMany: jest.fn().mockResolvedValue(new Map()),
      resolvePathIds: jest.fn(),
    };
    handler = new CatalogHandler(
      { esIndexPrefix: '' } as AppConfigService,
      writer,
      projection,
    );
  });

  const patchedCategoryIds = (): string[] =>
    writer.bulkPartialUpdate.mock.calls[0][1][0].doc.category_ids as string[];

  it('uses the resolved path of the primary category plus secondary categories', async () => {
    projection.resolvePathIds.mockImplementation(async (id: string) =>
      id === 'cat-leaf' ? ['cat-root', 'cat-leaf'] : [],
    );

    await handler.handle(
      event({
        product_id: 'prod-1',
        primary_category_id: 'cat-leaf',
        secondary_category_ids: ['cat-other'],
      }),
    );

    expect(patchedCategoryIds()).toEqual(['cat-root', 'cat-leaf', 'cat-other']);
  });

  it('indexes the leaf id when the category projection does not know it', async () => {
    projection.resolvePathIds.mockResolvedValue([]);

    await handler.handle(
      event({ product_id: 'prod-1', primary_category_id: 'cat-unknown' }),
    );

    expect(patchedCategoryIds()).toEqual(['cat-unknown']);
  });
});

describe('CatalogHandler product.shop_snapshot_updated', () => {
  it('updates every document of the shop when the event is keyed by shop', async () => {
    const writer = {
      bulkUpsert: jest.fn(),
      bulkPartialUpdate: jest.fn(),
      getCurrentVersions: jest.fn(),
      updateShopSnapshot: jest.fn().mockResolvedValue(12),
    } as jest.Mocked<IIndexWriter>;
    const handler = new CatalogHandler(
      { esIndexPrefix: '' } as AppConfigService,
      writer,
      { getMany: jest.fn(), resolvePathIds: jest.fn() },
    );

    // Payload đúng như Catalog shop-projection.service.ts phát ra.
    await handler.handle({
      event_id: 'evt-shop-1',
      schema_version: 1,
      event_type: 'product.shop_snapshot_updated',
      occurred_at: '2026-10-31T10:00:00Z',
      aggregate_type: 'SHOP_PROJECTION',
      aggregate_id: 'shop-1',
      version: 5,
      payload: {
        shop_id: 'shop-1',
        name: 'Anker Official',
        slug: 'anker-official',
        old_name: 'Anker',
        old_slug: 'anker',
      },
    });

    expect(writer.updateShopSnapshot).toHaveBeenCalledWith('products-read', {
      shop_id: 'shop-1',
      name: 'Anker Official',
    });
    expect(writer.bulkPartialUpdate).not.toHaveBeenCalled();
  });
});
