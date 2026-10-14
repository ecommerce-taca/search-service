import { VisibilityStatus } from '../../common/enums';
import { DomainEvent } from '../interfaces';
import {
  ICategoryProjection,
  IIndexWriter,
  IProductDocumentBuilder,
  ProductSearchDocument,
} from '../../product-index/interfaces';
import { ProductEventHandler } from './product-event.handler';

describe('ProductEventHandler', () => {
  let handler: ProductEventHandler;
  let mockBuilder: jest.Mocked<IProductDocumentBuilder>;
  let mockWriter: jest.Mocked<IIndexWriter>;
  let mockCategoryProjection: jest.Mocked<ICategoryProjection>;

  beforeEach(() => {
    mockBuilder = {
      build: jest.fn(),
    };

    mockWriter = {
      bulkUpsert: jest.fn().mockResolvedValue(undefined),
      bulkPartialUpdate: jest.fn().mockResolvedValue(undefined),
      getCurrentVersions: jest.fn().mockResolvedValue(new Map()),
    };

    mockCategoryProjection = {
      getMany: jest.fn().mockResolvedValue(new Map()),
      resolvePathIds: jest.fn().mockResolvedValue(['cat-root', 'cat-leaf']),
    };

    handler = new ProductEventHandler(
      mockBuilder,
      mockWriter,
      mockCategoryProjection,
    );
  });

  describe('[IDX-HND-01] category_ids resolution for product event', () => {
    it('[IDX-HND-01] should resolve category_ids via CATEGORY_PROJECTION and pass to builder/writer', async () => {
      const event: DomainEvent = {
        event_id: 'evt-1',
        schema_version: 1,
        event_type: 'product.published',
        occurred_at: '2026-10-14T10:00:00Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-1',
        version: 2,
        payload: {
          product_id: 'prod-1',
          primary_category_id: 'cat-leaf',
          title: 'Sản phẩm mới',
        },
      };

      const builtDoc: ProductSearchDocument = {
        product_id: 'prod-1',
        shop_id: 'shop-1',
        slug: 'san-pham-moi',
        title: 'Sản phẩm mới',
        description: 'Mô tả',
        category_ids: ['cat-root', 'cat-leaf'],
        category_path: ['root', 'leaf'],
        brand: 'Taca',
        price: 100000,
        currency: 'VND',
        rating_avg: 5,
        rating_count: 1,
        attributes: {},
        visibility_status: VisibilityStatus.PUBLISHED,
        source_version: 2,
        source_event_id: 'evt-1',
        indexed_at: '2026-10-14T10:00:00Z',
      };

      mockBuilder.build.mockReturnValue({
        kind: 'UPSERT',
        id: 'prod-1',
        doc: builtDoc,
      });

      await handler.handle(event);

      expect(mockCategoryProjection.resolvePathIds).toHaveBeenCalledWith('cat-leaf');
      expect(mockWriter.bulkUpsert).toHaveBeenCalledWith(
        expect.any(String),
        [builtDoc],
      );
    });
  });

  describe('[IDX-HND-02] handle PATCH changes', () => {
    it('[IDX-HND-02] should invoke bulkPartialUpdate when builder returns PATCH kind', async () => {
      const event: DomainEvent = {
        event_id: 'evt-2',
        schema_version: 1,
        event_type: 'product.updated',
        occurred_at: '2026-10-14T10:00:00Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-1',
        version: 3,
        payload: {
          product_id: 'prod-1',
          title: 'Tiêu đề cập nhật',
        },
      };

      mockBuilder.build.mockReturnValue({
        kind: 'PATCH',
        id: 'prod-1',
        doc: { title: 'Tiêu đề cập nhật' },
        sourceVersion: 3,
        sourceEventId: 'evt-2',
      });

      await handler.handle(event);

      expect(mockWriter.bulkPartialUpdate).toHaveBeenCalledWith(
        expect.any(String),
        [
          expect.objectContaining({
            id: 'prod-1',
            doc: expect.objectContaining({ title: 'Tiêu đề cập nhật' }),
          }),
        ],
      );
    });
  });

  describe('[IDX-HND-03] handle catalog events', () => {
    it('[IDX-HND-03] should handle product.media_updated by updating cover_url in ES document', async () => {
      const mediaEvent: DomainEvent = {
        event_id: 'evt-media-1',
        schema_version: 1,
        event_type: 'product.media_updated',
        occurred_at: '2026-10-14T10:00:00Z',
        aggregate_type: 'CATALOG',
        aggregate_id: 'prod-1',
        version: 4,
        payload: {
          product_id: 'prod-1',
          media: [{ url: 'https://cdn.example.com/updated.jpg' }],
        },
      };

      await handler.handle(mediaEvent);

      expect(mockWriter.bulkPartialUpdate).toHaveBeenCalledWith(
        expect.any(String),
        [
          expect.objectContaining({
            id: 'prod-1',
            doc: expect.objectContaining({
              cover_url: 'https://cdn.example.com/updated.jpg',
            }),
          }),
        ],
      );
    });
  });
});
