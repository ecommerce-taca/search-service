import { Client } from '@elastic/elasticsearch';
import { AppConfigService } from '../../config/config.service';
import { DomainEvent } from '../../event-consumer/interfaces';
import {
  CategoryProjection,
  ICategoryProjection,
  IIndexWriter,
} from '../interfaces';
import { CategoryProjectionWriter } from './category-projection.writer';

describe('CategoryProjectionWriter', () => {
  let writer: CategoryProjectionWriter;
  let clientMock: jest.Mocked<Partial<Client>>;
  let configServiceMock: jest.Mocked<Partial<AppConfigService>>;
  let categoryProjectionMock: jest.Mocked<ICategoryProjection>;
  let indexWriterMock: jest.Mocked<IIndexWriter>;

  beforeEach(() => {
    clientMock = {
      index: jest.fn().mockResolvedValue({ result: 'created' }),
      search: jest.fn(),
    };
    configServiceMock = {
      esIndexPrefix: 'test-',
    };
    categoryProjectionMock = {
      getMany: jest.fn().mockResolvedValue(new Map()),
      resolvePathIds: jest.fn().mockResolvedValue([]),
    };
    indexWriterMock = {
      bulkUpsert: jest.fn(),
      bulkPartialUpdate: jest.fn(),
      getCurrentVersions: jest.fn(),
    };

    writer = new CategoryProjectionWriter(
      clientMock as unknown as Client,
      configServiceMock as unknown as AppConfigService,
      categoryProjectionMock,
      indexWriterMock,
    );
  });

  it('upserts root category projection with path_ids = [categoryId]', async () => {
    const evt: DomainEvent = {
      event_id: 'cat-evt-1',
      schema_version: 1,
      event_type: 'category.created',
      occurred_at: '2026-10-14T10:00:00.000Z',
      aggregate_type: 'CATEGORY',
      aggregate_id: 'cat-root',
      version: 1,
      payload: {
        category_id: 'cat-root',
        name: 'Thời Trang',
        parent_id: null,
        status: 'ACTIVE',
      },
    };

    await writer.handleCategoryEvent(evt);

    expect(clientMock.index).toHaveBeenCalledWith(
      expect.objectContaining({
        index: 'test-search-categories',
        id: 'cat-root',
        version: 1,
        version_type: 'external_gte',
        document: expect.objectContaining({
          category_id: 'cat-root',
          name: 'Thời Trang',
          path_ids: ['cat-root'],
        }),
      }),
    );
  });

  it('resolves parent path_ids and appends child id', async () => {
    (categoryProjectionMock.resolvePathIds as jest.Mock).mockResolvedValue([
      'cat-root',
    ]);

    const evt: DomainEvent = {
      event_id: 'cat-evt-2',
      schema_version: 1,
      event_type: 'category.created',
      occurred_at: '2026-10-14T10:00:00.000Z',
      aggregate_type: 'CATEGORY',
      aggregate_id: 'cat-child',
      version: 1,
      payload: {
        category_id: 'cat-child',
        name: 'Áo Nam',
        parent_id: 'cat-root',
        status: 'ACTIVE',
      },
    };

    await writer.handleCategoryEvent(evt);

    expect(clientMock.index).toHaveBeenCalledWith(
      expect.objectContaining({
        document: expect.objectContaining({
          category_id: 'cat-child',
          path_ids: ['cat-root', 'cat-child'],
        }),
      }),
    );
  });

  it('skips event if version <= stored projection version', async () => {
    const existing: CategoryProjection = {
      category_id: 'cat-root',
      name: 'Thời Trang',
      parent_id: null,
      path_ids: ['cat-root'],
      status: 'ACTIVE',
      source_version: 5,
      updated_at: '2026-10-14T10:00:00.000Z',
    };
    (categoryProjectionMock.getMany as jest.Mock).mockResolvedValue(
      new Map([['cat-root', existing]]),
    );

    const olderEvt: DomainEvent = {
      event_id: 'cat-evt-3',
      schema_version: 1,
      event_type: 'category.updated',
      occurred_at: '2026-10-14T10:00:00.000Z',
      aggregate_type: 'CATEGORY',
      aggregate_id: 'cat-root',
      version: 4,
      payload: { category_id: 'cat-root' },
    };

    await writer.handleCategoryEvent(olderEvt);

    expect(clientMock.index).not.toHaveBeenCalled();
  });

  it('re-resolves affected products on path change', async () => {
    const existing: CategoryProjection = {
      category_id: 'cat-child',
      name: 'Áo Nam',
      parent_id: 'cat-old-parent',
      path_ids: ['cat-old-parent', 'cat-child'],
      status: 'ACTIVE',
      source_version: 1,
      updated_at: '2026-10-14T10:00:00.000Z',
    };
    (categoryProjectionMock.getMany as jest.Mock).mockResolvedValue(
      new Map([['cat-child', existing]]),
    );

    // Parent path changed to new parent
    (categoryProjectionMock.resolvePathIds as jest.Mock).mockImplementation(
      async (id: string) => {
        if (id === 'cat-new-parent') return ['cat-new-parent'];
        if (id === 'cat-child') return ['cat-new-parent', 'cat-child'];
        return [];
      },
    );

    (clientMock.search as jest.Mock).mockResolvedValue({
      hits: {
        hits: [
          {
            _id: 'prod-10',
            _source: {
              product_id: 'prod-10',
              category_ids: ['cat-old-parent', 'cat-child'],
            },
            sort: ['prod-10'],
          },
        ],
      },
    });

    const evt: DomainEvent = {
      event_id: 'cat-evt-4',
      schema_version: 1,
      event_type: 'category.updated',
      occurred_at: '2026-10-14T10:00:00.000Z',
      aggregate_type: 'CATEGORY',
      aggregate_id: 'cat-child',
      version: 2,
      payload: {
        category_id: 'cat-child',
        name: 'Áo Nam',
        parent_id: 'cat-new-parent',
      },
    };

    await writer.handleCategoryEvent(evt);

    expect(indexWriterMock.bulkPartialUpdate).toHaveBeenCalledWith(
      'test-products-read',
      [
        {
          id: 'prod-10',
          doc: expect.objectContaining({
            category_ids: ['cat-new-parent', 'cat-child'],
          }),
        },
      ],
    );
  });
});
