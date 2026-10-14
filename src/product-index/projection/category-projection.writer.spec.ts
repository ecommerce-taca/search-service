import { Client } from '@elastic/elasticsearch';
import { ICategoryProjection, IIndexWriter } from '../interfaces';
import { CategoryProjectionWriter } from './category-projection.writer';

describe('CategoryProjectionWriter', () => {
  let writer: CategoryProjectionWriter;
  let mockClient: jest.Mocked<Partial<Client>>;
  let mockCategoryProjection: jest.Mocked<ICategoryProjection>;
  let mockIndexWriter: jest.Mocked<IIndexWriter>;

  beforeEach(() => {
    mockClient = {
      index: jest.fn().mockResolvedValue({ result: 'updated' }),
      get: jest.fn().mockResolvedValue({
        found: true,
        _source: {
          category_id: 'cat-phones',
          name: 'Điện thoại',
          parent_id: 'cat-root',
          path_ids: ['cat-root', 'cat-phones'],
          status: 'ACTIVE',
          source_version: 1,
        },
      }),
      search: jest.fn().mockResolvedValue({
        hits: {
          hits: [],
          total: { value: 0 },
        },
      }),
    };

    mockCategoryProjection = {
      getMany: jest.fn().mockImplementation(async (ids: string[]) => {
        const map = new Map();
        if (ids.includes('cat-root')) {
          map.set('cat-root', {
            category_id: 'cat-root',
            name: 'Thiết bị điện tử',
            parent_id: null,
            path_ids: ['cat-root'],
            status: 'ACTIVE',
            source_version: 1,
            updated_at: '2026-10-14T10:00:00Z',
          });
        }
        if (ids.includes('cat-phones')) {
          map.set('cat-phones', {
            category_id: 'cat-phones',
            name: 'Điện thoại',
            parent_id: 'cat-root',
            path_ids: ['cat-root', 'cat-phones'],
            status: 'ACTIVE',
            source_version: 1,
            updated_at: '2026-10-14T10:00:00Z',
          });
        }
        return map;
      }),
      resolvePathIds: jest.fn().mockImplementation(async (id: string) => {
        if (id === 'cat-root') return ['cat-root'];
        if (id === 'cat-phones') return ['cat-root', 'cat-phones'];
        if (id === 'cat-new-parent') return ['cat-root', 'cat-new-parent'];
        return [id];
      }),
    };

    mockIndexWriter = {
      bulkUpsert: jest.fn().mockResolvedValue(undefined),
      bulkPartialUpdate: jest.fn().mockResolvedValue(undefined),
      getCurrentVersions: jest.fn().mockResolvedValue(new Map()),
    };

    writer = new CategoryProjectionWriter(
      mockClient as unknown as Client,
      mockCategoryProjection,
      mockIndexWriter,
    );
  });

  describe('[IDX-CAT-01] category creation and update', () => {
    it('[IDX-CAT-01] should upsert category projection document with version_type: external_gte', async () => {
      const event = {
        category_id: 'cat-phones',
        name: 'Điện thoại thông minh',
        parent_id: 'cat-root',
        status: 'ACTIVE',
        version: 2,
        updated_at: '2026-10-14T10:00:00Z',
      };

      await writer.handleCategoryEvent(event);

      expect(mockClient.index).toHaveBeenCalledWith(
        expect.objectContaining({
          id: 'cat-phones',
          version_type: 'external_gte',
          version: 2,
          document: expect.objectContaining({
            category_id: 'cat-phones',
            name: 'Điện thoại thông minh',
            parent_id: 'cat-root',
            path_ids: ['cat-root', 'cat-phones'],
            status: 'ACTIVE',
            source_version: 2,
          }),
        }),
      );
    });

    it('[IDX-CAT-01] should set path_ids as [category_id] for root category (parent_id is null)', async () => {
      const rootEvent = {
        category_id: 'cat-root',
        name: 'Thiết bị điện tử',
        parent_id: null,
        status: 'ACTIVE',
        version: 1,
        updated_at: '2026-10-14T10:00:00Z',
      };

      await writer.handleCategoryEvent(rootEvent);

      expect(mockClient.index).toHaveBeenCalledWith(
        expect.objectContaining({
          id: 'cat-root',
          document: expect.objectContaining({
            category_id: 'cat-root',
            path_ids: ['cat-root'],
            parent_id: null,
          }),
        }),
      );
    });
  });

  describe('[IDX-CAT-02] path change and affected product re-indexing', () => {
    it('[IDX-CAT-02] should query affected products with search_after batching and bulk update category_ids when path changes', async () => {
      // Existing projection has parent cat-root -> path ['cat-root', 'cat-phones']
      // New event changes parent to cat-new-parent -> new path ['cat-root', 'cat-new-parent', 'cat-phones']
      (mockClient.get as jest.Mock).mockResolvedValueOnce({
        found: true,
        _source: {
          category_id: 'cat-phones',
          name: 'Điện thoại',
          parent_id: 'cat-root',
          path_ids: ['cat-root', 'cat-phones'],
          source_version: 1,
        },
      });

      // Products search returns 2 hits
      (mockClient.search as jest.Mock)
        .mockResolvedValueOnce({
          hits: {
            hits: [
              {
                _id: 'prod-1',
                _source: {
                  product_id: 'prod-1',
                  category_ids: ['cat-root', 'cat-phones', 'cat-leaf'],
                  primary_category_id: 'cat-leaf',
                },
                sort: ['prod-1'],
              },
            ],
          },
        })
        .mockResolvedValueOnce({
          hits: {
            hits: [], // end of search_after
          },
        });

      const pathChangeEvent = {
        category_id: 'cat-phones',
        name: 'Điện thoại',
        parent_id: 'cat-new-parent',
        status: 'ACTIVE',
        version: 2,
        updated_at: '2026-10-14T10:00:00Z',
      };

      await writer.handleCategoryEvent(pathChangeEvent);

      // Verify search was invoked to find affected products
      expect(mockClient.search).toHaveBeenCalledWith(
        expect.objectContaining({
          query: expect.objectContaining({
            term: { category_ids: 'cat-phones' },
          }),
        }),
      );

      // Verify bulk partial update was called without using painless scripts
      expect(mockIndexWriter.bulkPartialUpdate).toHaveBeenCalled();
    });
  });

  describe('[IDX-CAT-03] name change without path change', () => {
    it('[IDX-CAT-03] should not search or re-index products when only category name changes', async () => {
      const nameChangeEvent = {
        category_id: 'cat-phones',
        name: 'Điện thoại di động (Đổi tên)',
        parent_id: 'cat-root', // parent unchanged
        status: 'ACTIVE',
        version: 2,
        updated_at: '2026-10-14T10:00:00Z',
      };

      await writer.handleCategoryEvent(nameChangeEvent);

      expect(mockClient.index).toHaveBeenCalled();
      expect(mockClient.search).not.toHaveBeenCalled();
      expect(mockIndexWriter.bulkPartialUpdate).not.toHaveBeenCalled();
    });
  });

  describe('[IDX-CAT-04] version conflict or stale version', () => {
    it('[IDX-CAT-04] should skip category update when incoming version is older than stored projection version', async () => {
      (mockCategoryProjection.getMany as jest.Mock).mockResolvedValueOnce(
        new Map([
          [
            'cat-phones',
            {
              category_id: 'cat-phones',
              name: 'Điện thoại',
              parent_id: 'cat-root',
              path_ids: ['cat-root', 'cat-phones'],
              status: 'ACTIVE',
              source_version: 5,
            },
          ],
        ]),
      );

      const staleEvent = {
        category_id: 'cat-phones',
        name: 'Điện thoại cũ',
        parent_id: 'cat-root',
        status: 'ACTIVE',
        version: 3, // older than 5
        updated_at: '2026-10-14T08:00:00Z',
      };

      await writer.handleCategoryEvent(staleEvent);

      expect(mockClient.index).not.toHaveBeenCalled();
    });
  });
});
