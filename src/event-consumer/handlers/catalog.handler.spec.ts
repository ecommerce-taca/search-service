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
