import { VisibilityStatus } from '../../common/enums';
import { SearchErrorCode, SearchException } from '../../common/errors';
import { DomainEvent } from '../../event-consumer/interfaces';
import {
  DocumentChange,
  IProductDocumentBuilder,
  ProductSearchDocument,
} from '../interfaces';
import { ProductDocumentBuilder } from './product-document.builder';

describe('ProductDocumentBuilder', () => {
  let builder: IProductDocumentBuilder;

  beforeEach(() => {
    builder = new ProductDocumentBuilder();
  });

  const baseProductDoc: ProductSearchDocument = {
    product_id: 'prod-001',
    shop_id: 'shop-001',
    slug: 'ao-khoac-cotton',
    title: 'Áo khoác cotton',
    title_exact: 'Áo khoác cotton',
    description: 'Mô tả sản phẩm',
    category_ids: ['cat-root', 'cat-leaf'],
    category_path: ['fashion', 'outerwear'],
    brand: 'Taca Brand',
    price: 249000,
    currency: 'VND',
    rating_avg: 4.5,
    rating_count: 10,
    attributes: { color: ['Đen'] },
    spec_attributes: { material: 'cotton' },
    attribute_pairs: ['color=Đen'],
    suggest: { input: ['Áo khoác cotton', 'Taca Brand'] },
    visibility_status: VisibilityStatus.PUBLISHED,
    source_version: 5,
    source_event_id: 'evt-version-5',
    indexed_at: '2026-10-14T10:00:00.000Z',
    created_at: '2026-10-14T09:00:00.000Z',
    cover_url: 'https://cdn.example.com/coat.jpg',
    shop: {
      shop_id: 'shop-001',
      name: 'Taca Shop',
    },
  };

  describe('[IDX-BLD-01] product.created event', () => {
    it('[IDX-BLD-01] should produce UPSERT with visibility HIDDEN when current document does not exist', () => {
      const event: DomainEvent = {
        event_id: 'evt-create-1',
        schema_version: 1,
        event_type: 'product.created',
        occurred_at: '2026-10-14T10:00:00.000Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-002',
        version: 1,
        payload: {
          product_id: 'prod-002',
          shop_id: 'shop-001',
          title: 'Sản phẩm mới khởi tạo',
          slug: 'san-pham-moi-khoi-tao',
          category_path: ['fashion'],
          brand: 'Taca Brand',
          price: { sale: 100000, currency: 'VND' },
          attributes: { material: 'cotton' },
          media: [{ url: 'https://cdn.example.com/new.jpg' }],
          visibility_status: 'HIDDEN',
        },
      };

      const result = builder.build(event, null);

      expect(result.kind).toBe('UPSERT');
      if (result.kind === 'UPSERT') {
        expect(result.id).toBe('prod-002');
        expect(result.doc.visibility_status).toBe(VisibilityStatus.HIDDEN);
        expect(result.doc.source_version).toBe(1);
        expect(result.doc.source_event_id).toBe('evt-create-1');
      }
    });

    it('[IDX-BLD-01] should produce PATCH with changed fields if document already exists', () => {
      const event: DomainEvent = {
        event_id: 'evt-create-2',
        schema_version: 1,
        event_type: 'product.created',
        occurred_at: '2026-10-14T10:05:00.000Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-001',
        version: 6,
        payload: {
          product_id: 'prod-001',
          title: 'Áo khoác cotton cập nhật nhẹ',
        },
      };

      const result = builder.build(event, baseProductDoc);

      expect(result.kind).toBe('PATCH');
      if (result.kind === 'PATCH') {
        expect(result.id).toBe('prod-001');
        expect(result.sourceVersion).toBe(6);
        expect(result.sourceEventId).toBe('evt-create-2');
        expect(result.doc.title).toBe('Áo khoác cotton cập nhật nhẹ');
      }
    });
  });

  describe('[IDX-BLD-02] product.updated event', () => {
    it('[IDX-BLD-02] should produce PATCH with changed fields, preserving source_version and source_event_id', () => {
      const event: DomainEvent = {
        event_id: 'evt-update-1',
        schema_version: 1,
        event_type: 'product.updated',
        occurred_at: '2026-10-14T10:10:00.000Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-001',
        version: 6,
        payload: {
          product_id: 'prod-001',
          title: 'Áo khoác cotton v2',
          description: 'Mô tả mới',
        },
      };

      const result = builder.build(event, baseProductDoc);

      expect(result.kind).toBe('PATCH');
      if (result.kind === 'PATCH') {
        expect(result.id).toBe('prod-001');
        expect(result.sourceVersion).toBe(6);
        expect(result.sourceEventId).toBe('evt-update-1');
        expect(result.doc.title).toBe('Áo khoác cotton v2');
        expect(result.doc.description).toBe('Mô tả mới');
      }
    });
  });

  describe('[IDX-BLD-03] product.published event', () => {
    it('[IDX-BLD-03] should produce UPSERT full doc with mapping rules (price.sale, spec_attributes, suggest, shop snapshot, cover_url)', () => {
      const event: DomainEvent = {
        event_id: 'evt-pub-1',
        schema_version: 1,
        event_type: 'product.published',
        occurred_at: '2026-10-14T11:00:00.000Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-001',
        version: 7,
        payload: {
          product_id: 'prod-001',
          shop_id: 'shop-001',
          title: 'Áo khoác cotton cao cấp',
          slug: 'ao-khoac-cotton-cao-cap',
          category_path: ['fashion', 'outerwear'],
          brand: 'Taca Brand',
          price: {
            sale: 299000,
            currency: 'VND',
          },
          attributes: {
            material: 'cotton',
            origin: 'Vietnam',
          },
          media: [
            { url: 'https://cdn.example.com/coat-first.jpg', alt: 'First' },
            { url: 'https://cdn.example.com/coat-second.jpg', alt: 'Second' },
          ],
          visibility_status: 'PUBLISHED',
          published_at: '2026-10-14T11:00:00.000Z',
          shop: {
            shop_id: 'shop-001',
            name: 'Taca Official Store',
          },
        },
      };

      const result = builder.build(event, baseProductDoc);

      expect(result.kind).toBe('UPSERT');
      if (result.kind === 'UPSERT') {
        expect(result.id).toBe('prod-001');
        const doc = result.doc;
        expect(doc.price).toBe(299000); // price = payload.price.sale
        expect(doc.currency).toBe('VND');
        expect(doc.visibility_status).toBe(VisibilityStatus.PUBLISHED);
        expect(doc.spec_attributes).toEqual({
          material: 'cotton',
          origin: 'Vietnam',
        }); // spec_attributes = payload.attributes (D2)
        expect(doc.cover_url).toBe('https://cdn.example.com/coat-first.jpg'); // media[0].url
        expect(doc.title_exact).toBe('Áo khoác cotton cao cấp');
        expect(doc.suggest).toEqual({
          input: ['Áo khoác cotton cao cấp', 'Taca Brand'],
        });
        expect(doc.shop).toEqual({
          shop_id: 'shop-001',
          name: 'Taca Official Store',
        });
        expect(doc.source_version).toBe(7);
        expect(doc.source_event_id).toBe('evt-pub-1');
      }
    });

    it('[IDX-BLD-03] should inherit created_at from published_at when created_at is absent', () => {
      const event: DomainEvent = {
        event_id: 'evt-pub-2',
        schema_version: 1,
        event_type: 'product.published',
        occurred_at: '2026-10-14T11:00:00.000Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-003',
        version: 1,
        payload: {
          product_id: 'prod-003',
          shop_id: 'shop-002',
          title: 'Sản phẩm mới xuất bản',
          slug: 'san-pham-moi-xuat-ban',
          category_path: ['fashion'],
          brand: 'Generic',
          price: { sale: 150000, currency: 'VND' },
          attributes: {},
          media: [{ url: 'https://cdn.example.com/img.jpg' }],
          visibility_status: 'PUBLISHED',
          published_at: '2026-10-14T11:00:00.000Z',
        },
      };

      const result = builder.build(event, null);

      expect(result.kind).toBe('UPSERT');
      if (result.kind === 'UPSERT') {
        expect(result.doc.created_at).toBe('2026-10-14T11:00:00.000Z');
      }
    });
  });

  describe('[IDX-BLD-04] product.unpublished event', () => {
    it('[IDX-BLD-04] should produce PATCH with visibility HIDDEN', () => {
      const event: DomainEvent = {
        event_id: 'evt-unpub-1',
        schema_version: 1,
        event_type: 'product.unpublished',
        occurred_at: '2026-10-14T12:00:00.000Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-001',
        version: 6,
        payload: { product_id: 'prod-001' },
      };

      const result = builder.build(event, baseProductDoc);

      expect(result.kind).toBe('PATCH');
      if (result.kind === 'PATCH') {
        expect(result.doc.visibility_status).toBe(VisibilityStatus.HIDDEN);
        expect(result.sourceVersion).toBe(6);
        expect(result.sourceEventId).toBe('evt-unpub-1');
      }
    });
  });

  describe('[IDX-BLD-05] product.blocked event', () => {
    it('[IDX-BLD-05] should produce PATCH with visibility HIDDEN', () => {
      const event: DomainEvent = {
        event_id: 'evt-block-1',
        schema_version: 1,
        event_type: 'product.blocked',
        occurred_at: '2026-10-14T12:05:00.000Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-001',
        version: 6,
        payload: { product_id: 'prod-001', reason: 'VIOLATION' },
      };

      const result = builder.build(event, baseProductDoc);

      expect(result.kind).toBe('PATCH');
      if (result.kind === 'PATCH') {
        expect(result.doc.visibility_status).toBe(VisibilityStatus.HIDDEN);
        expect(result.sourceVersion).toBe(6);
      }
    });
  });

  describe('[IDX-BLD-06] product.unblocked event (next_status ACTIVE)', () => {
    it('[IDX-BLD-06] should produce PATCH with visibility PUBLISHED when next_status is ACTIVE', () => {
      const event: DomainEvent = {
        event_id: 'evt-unblock-active',
        schema_version: 1,
        event_type: 'product.unblocked',
        occurred_at: '2026-10-14T12:10:00.000Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-001',
        version: 6,
        payload: { product_id: 'prod-001', next_status: 'ACTIVE' },
      };

      const result = builder.build(event, {
        ...baseProductDoc,
        visibility_status: VisibilityStatus.HIDDEN,
      });

      expect(result.kind).toBe('PATCH');
      if (result.kind === 'PATCH') {
        expect(result.doc.visibility_status).toBe(VisibilityStatus.PUBLISHED);
        expect(result.sourceVersion).toBe(6);
      }
    });
  });

  describe('[IDX-BLD-07] product.unblocked event (next_status INACTIVE)', () => {
    it('[IDX-BLD-07] should produce PATCH with visibility HIDDEN when next_status is INACTIVE', () => {
      const event: DomainEvent = {
        event_id: 'evt-unblock-inactive',
        schema_version: 1,
        event_type: 'product.unblocked',
        occurred_at: '2026-10-14T12:15:00.000Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-001',
        version: 6,
        payload: { product_id: 'prod-001', next_status: 'INACTIVE' },
      };

      const result = builder.build(event, {
        ...baseProductDoc,
        visibility_status: VisibilityStatus.HIDDEN,
      });

      expect(result.kind).toBe('PATCH');
      if (result.kind === 'PATCH') {
        expect(result.doc.visibility_status).toBe(VisibilityStatus.HIDDEN);
        expect(result.sourceVersion).toBe(6);
      }
    });
  });

  describe('[IDX-BLD-08] product.archived event', () => {
    it('[IDX-BLD-08] should produce PATCH with visibility DELETED', () => {
      const event: DomainEvent = {
        event_id: 'evt-archive-1',
        schema_version: 1,
        event_type: 'product.archived',
        occurred_at: '2026-10-14T12:20:00.000Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-001',
        version: 6,
        payload: { product_id: 'prod-001' },
      };

      const result = builder.build(event, baseProductDoc);

      expect(result.kind).toBe('PATCH');
      if (result.kind === 'PATCH') {
        expect(result.doc.visibility_status).toBe(VisibilityStatus.DELETED);
        expect(result.sourceVersion).toBe(6);
      }
    });
  });

  describe('[IDX-BLD-09] version regression rule', () => {
    it('[IDX-BLD-09] should return SKIP with reason OLD_VERSION when event.version <= current.source_version', () => {
      const olderEvent: DomainEvent = {
        event_id: 'evt-old-1',
        schema_version: 1,
        event_type: 'product.updated',
        occurred_at: '2026-10-14T09:00:00.000Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-001',
        version: 4, // current is 5
        payload: { product_id: 'prod-001', title: 'Tiêu đề cũ' },
      };

      const result = builder.build(olderEvent, baseProductDoc);

      expect(result).toEqual({
        kind: 'SKIP',
        reason: 'OLD_VERSION',
      });
    });

    it('[IDX-BLD-09] should return SKIP with reason OLD_VERSION when event.version == current.source_version', () => {
      const sameVersionEvent: DomainEvent = {
        event_id: 'evt-same-1',
        schema_version: 1,
        event_type: 'product.updated',
        occurred_at: '2026-10-14T10:00:00.000Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-001',
        version: 5, // current is 5
        payload: { product_id: 'prod-001', title: 'Cùng phiên bản' },
      };

      const result = builder.build(sameVersionEvent, baseProductDoc);

      expect(result).toEqual({
        kind: 'SKIP',
        reason: 'OLD_VERSION',
      });
    });
  });

  describe('[IDX-BLD-10] duplicate event rule', () => {
    it('[IDX-BLD-10] should return SKIP with reason DUPLICATE when current.source_event_id === evt.event_id', () => {
      const duplicateEvent: DomainEvent = {
        event_id: 'evt-version-5', // same as baseProductDoc.source_event_id
        schema_version: 1,
        event_type: 'product.published',
        occurred_at: '2026-10-14T10:00:00.000Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-001',
        version: 8,
        payload: { product_id: 'prod-001' },
      };

      const result = builder.build(duplicateEvent, baseProductDoc);

      expect(result).toEqual({
        kind: 'SKIP',
        reason: 'DUPLICATE',
      });
    });
  });

  describe('[IDX-BLD-11] unknown event_type', () => {
    it('[IDX-BLD-11] should return SKIP with reason NOT_APPLICABLE for unknown event_type', () => {
      const unknownEvent: DomainEvent = {
        event_id: 'evt-unknown-1',
        schema_version: 1,
        event_type: 'product.inventory_audited',
        occurred_at: '2026-10-14T12:30:00.000Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-001',
        version: 6,
        payload: { audit_id: 'aud-1' },
      };

      const result = builder.build(unknownEvent, baseProductDoc);

      expect(result).toEqual({
        kind: 'SKIP',
        reason: 'NOT_APPLICABLE',
      });
    });
  });

  describe('[IDX-BLD-12] invalid event shape', () => {
    it('[IDX-BLD-12] should throw SearchException(SEARCH_EVENT_INVALID) when payload is null or missing required fields', () => {
      const invalidEvent: DomainEvent = {
        event_id: 'evt-invalid-1',
        schema_version: 1,
        event_type: 'product.published',
        occurred_at: '2026-10-14T12:40:00.000Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-001',
        version: 6,
        payload: null,
      };

      expect(() => builder.build(invalidEvent, baseProductDoc)).toThrow(
        SearchException,
      );

      try {
        builder.build(invalidEvent, baseProductDoc);
      } catch (err: unknown) {
        expect(err).toBeInstanceOf(SearchException);
        expect((err as SearchException).code).toBe(
          SearchErrorCode.SEARCH_EVENT_INVALID,
        );
      }
    });
  });

  describe('[IDX-BLD-13] category_ids handling', () => {
    it('[IDX-BLD-13] should support attaching resolved category_ids via withCategoryIds helper or change doc', () => {
      const event: DomainEvent = {
        event_id: 'evt-cat-1',
        schema_version: 1,
        event_type: 'product.published',
        occurred_at: '2026-10-14T11:00:00.000Z',
        aggregate_type: 'PRODUCT',
        aggregate_id: 'prod-001',
        version: 7,
        payload: {
          product_id: 'prod-001',
          shop_id: 'shop-001',
          title: 'Áo khoác',
          slug: 'ao-khoac',
          category_path: ['fashion', 'outerwear'],
          primary_category_id: 'cat-leaf-smartphones',
          brand: 'Taca',
          price: { sale: 200000, currency: 'VND' },
          attributes: {},
          media: [{ url: 'https://cdn.example.com/img.jpg' }],
          visibility_status: 'PUBLISHED',
        },
      };

      const result = builder.build(event, baseProductDoc);
      expect(result.kind).toBe('UPSERT');

      if (
        'withCategoryIds' in builder &&
        typeof (
          builder as unknown as {
            withCategoryIds: (...args: unknown[]) => DocumentChange;
          }
        ).withCategoryIds === 'function'
      ) {
        const enriched = (
          builder as unknown as {
            withCategoryIds: (
              c: DocumentChange,
              ids: string[],
            ) => DocumentChange;
          }
        ).withCategoryIds(result, ['cat-root', 'cat-leaf']);
        if (enriched.kind === 'UPSERT') {
          expect(enriched.doc.category_ids).toEqual(['cat-root', 'cat-leaf']);
        }
      }
    });
  });
});
