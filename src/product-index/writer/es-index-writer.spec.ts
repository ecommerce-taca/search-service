import { Client } from '@elastic/elasticsearch';
import { ES_BULK_BATCH_SIZE } from '../../common/constants';
import { VisibilityStatus } from '../../common/enums';
import { IIndexWriter, ProductSearchDocument } from '../interfaces';
import { EsIndexWriter } from './es-index-writer';

describe('EsIndexWriter', () => {
  let writer: IIndexWriter;
  let mockClient: jest.Mocked<Partial<Client>>;

  beforeEach(() => {
    mockClient = {
      bulk: jest.fn().mockResolvedValue({
        errors: false,
        items: [],
      }),
      mget: jest.fn().mockResolvedValue({
        docs: [],
      }),
      search: jest.fn().mockResolvedValue({
        hits: { hits: [] },
      }),
    };

    writer = new EsIndexWriter(mockClient as unknown as Client);
  });

  const createDummyDoc = (id: string, version: number): ProductSearchDocument => ({
    product_id: id,
    shop_id: 'shop-001',
    slug: `slug-${id}`,
    title: `Sản phẩm ${id}`,
    title_exact: `Sản phẩm ${id}`,
    description: `Mô tả ${id}`,
    category_ids: ['cat-root'],
    category_path: ['fashion'],
    brand: 'Taca',
    price: 100000,
    currency: 'VND',
    rating_avg: 5.0,
    rating_count: 1,
    attributes: {},
    spec_attributes: {},
    attribute_pairs: [],
    visibility_status: VisibilityStatus.PUBLISHED,
    source_version: version,
    source_event_id: `evt-${id}`,
    indexed_at: '2026-10-14T10:00:00.000Z',
  });

  describe('[IDX-WRT-01] bulkUpsert optimistic locking', () => {
    it('[IDX-WRT-01] should send bulk index operations with version_type: external_gte and version: source_version', async () => {
      const doc = createDummyDoc('prod-1', 10);
      await writer.bulkUpsert('products-v1', [doc]);

      expect(mockClient.bulk).toHaveBeenCalledTimes(1);
      const bulkCallArgs = (mockClient.bulk as jest.Mock).mock.calls[0][0];

      expect(bulkCallArgs.index).toBe('products-v1');
      // operations: [ { index: { _index, _id, version_type: 'external_gte', version: 10 } }, doc ]
      const operations = bulkCallArgs.operations;
      expect(operations).toBeDefined();
      expect(operations.length).toBe(2);

      const opHeader = operations[0];
      const headerBody = opHeader.index || opHeader.create;
      expect(headerBody).toBeDefined();
      expect(headerBody._id).toBe('prod-1');
      expect(headerBody.version_type).toBe('external_gte');
      expect(headerBody.version).toBe(10);

      const opDoc = operations[1];
      expect(opDoc.product_id).toBe('prod-1');
    });
  });

  describe('[IDX-WRT-02] bulkUpsert chunking for large inputs', () => {
    it('[IDX-WRT-02] should chunk 1201 documents into 3 bulk calls (500, 500, 201)', async () => {
      const docs: ProductSearchDocument[] = [];
      for (let i = 0; i < 1201; i++) {
        docs.push(createDummyDoc(`prod-${i}`, i + 1));
      }

      await writer.bulkUpsert('products-v1', docs);

      expect(mockClient.bulk).toHaveBeenCalledTimes(3);

      const firstCallOps = (mockClient.bulk as jest.Mock).mock.calls[0][0].operations;
      const secondCallOps = (mockClient.bulk as jest.Mock).mock.calls[1][0].operations;
      const thirdCallOps = (mockClient.bulk as jest.Mock).mock.calls[2][0].operations;

      // Each document takes 2 array items (action metadata + document body)
      expect(firstCallOps.length).toBe(500 * 2);
      expect(secondCallOps.length).toBe(500 * 2);
      expect(thirdCallOps.length).toBe(201 * 2);
    });

    it('[IDX-WRT-02] should handle empty docs array without calling bulk', async () => {
      await writer.bulkUpsert('products-v1', []);
      expect(mockClient.bulk).not.toHaveBeenCalled();
    });
  });

  describe('[IDX-WRT-03] version conflict 409 handling', () => {
    it('[IDX-WRT-03] should treat 409 version conflict item response as SKIP rather than throwing an error', async () => {
      mockClient.bulk = jest.fn().mockResolvedValue({
        errors: true,
        items: [
          {
            index: {
              _id: 'prod-conflict',
              status: 409,
              error: {
                type: 'version_conflict_engine_exception',
                reason: '[prod-conflict]: version conflict, current version [10] is higher or equal to provided [8]',
              },
            },
          },
          {
            index: {
              _id: 'prod-ok',
              status: 200,
            },
          },
        ],
      });

      const doc1 = createDummyDoc('prod-conflict', 8);
      const doc2 = createDummyDoc('prod-ok', 12);

      // Should complete without throwing exception
      await expect(writer.bulkUpsert('products-v1', [doc1, doc2])).resolves.toBeUndefined();
    });
  });

  describe('[IDX-WRT-04] error handling on unrecoverable ES errors', () => {
    it('[IDX-WRT-04] should throw or handle critical errors when bulk response contains 500 error items', async () => {
      mockClient.bulk = jest.fn().mockResolvedValue({
        errors: true,
        items: [
          {
            index: {
              _id: 'prod-fail',
              status: 500,
              error: {
                type: 'circuit_breaking_exception',
                reason: 'Data too large',
              },
            },
          },
        ],
      });

      const doc = createDummyDoc('prod-fail', 1);
      await expect(writer.bulkUpsert('products-v1', [doc])).rejects.toThrow();
    });
  });

  describe('[IDX-WRT-05] refresh interval policy', () => {
    it('[IDX-WRT-05] should not pass refresh: true to avoid per-document refresh overhead', async () => {
      const doc = createDummyDoc('prod-1', 1);
      await writer.bulkUpsert('products-v1', [doc]);

      const bulkCallArgs = (mockClient.bulk as jest.Mock).mock.calls[0][0];
      expect(bulkCallArgs.refresh).toBeFalsy();
    });
  });

  describe('[IDX-WRT-06] bulkPartialUpdate', () => {
    it('[IDX-WRT-06] should send bulk update operations with doc payload', async () => {
      const updates = [
        {
          id: 'prod-1',
          doc: { title: 'Tiêu đề cập nhật' },
        },
      ];

      await writer.bulkPartialUpdate('products-v1', updates);

      expect(mockClient.bulk).toHaveBeenCalledTimes(1);
      const bulkCallArgs = (mockClient.bulk as jest.Mock).mock.calls[0][0];
      const operations = bulkCallArgs.operations;

      expect(operations.length).toBe(2);
      const opHeader = operations[0];
      expect(opHeader.update).toBeDefined();
      expect(opHeader.update._id).toBe('prod-1');

      const opBody = operations[1];
      expect(opBody.doc).toEqual({ title: 'Tiêu đề cập nhật' });
    });

    it('[IDX-WRT-06] should chunk partial updates if input exceeds ES_BULK_BATCH_SIZE', async () => {
      const updates = Array.from({ length: 600 }, (_, i) => ({
        id: `prod-${i}`,
        doc: { visibility_status: VisibilityStatus.HIDDEN },
      }));

      await writer.bulkPartialUpdate('products-v1', updates);

      expect(mockClient.bulk).toHaveBeenCalledTimes(2);
    });
  });

  describe('[IDX-WRT-07] getCurrentVersions', () => {
    it('[IDX-WRT-07] should fetch and map current source_version for given document ids', async () => {
      mockClient.mget = jest.fn().mockResolvedValue({
        docs: [
          {
            _id: 'prod-1',
            found: true,
            _source: { source_version: 5 },
          },
          {
            _id: 'prod-2',
            found: true,
            _source: { source_version: 12 },
          },
          {
            _id: 'prod-missing',
            found: false,
          },
        ],
      });

      const versions = await writer.getCurrentVersions('products-v1', [
        'prod-1',
        'prod-2',
        'prod-missing',
      ]);

      expect(versions.size).toBe(2);
      expect(versions.get('prod-1')).toBe(5);
      expect(versions.get('prod-2')).toBe(12);
      expect(versions.has('prod-missing')).toBe(false);
    });

    it('[IDX-WRT-07] should return empty map when ids array is empty without calling ES', async () => {
      const versions = await writer.getCurrentVersions('products-v1', []);
      expect(versions.size).toBe(0);
      expect(mockClient.mget).not.toHaveBeenCalled();
    });
  });
});
