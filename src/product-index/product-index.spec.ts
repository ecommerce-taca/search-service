import { Client } from '@elastic/elasticsearch';
import { VisibilityStatus } from '../common/enums';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { AppConfigService } from '../config/config.service';
import { DomainEvent } from '../event-consumer/interfaces';
import { ProductDocumentBuilder } from './document-builder/product-document.builder';
import {
  CategoryProjection,
  ICategoryProjection,
  IIndexWriter,
  ProductSearchDocument,
} from './interfaces';
import { CategoryProjectionWriter } from './projection/category-projection.writer';
import { VariantAggregator } from './sku/variant-aggregator';
import { EsIndexWriter } from './writer/es-index-writer';

describe('ProductIndexModule Unit Tests', () => {
  describe('ProductDocumentBuilder', () => {
    let builder: ProductDocumentBuilder;

    beforeEach(() => {
      builder = new ProductDocumentBuilder();
    });

    const baseEvent: DomainEvent = {
      event_id: 'evt-100',
      schema_version: 1,
      event_type: 'product.created',
      occurred_at: '2026-10-14T10:00:00.000Z',
      aggregate_type: 'PRODUCT',
      aggregate_id: 'prod-1',
      version: 2,
      payload: {
        product_id: 'prod-1',
        title: 'Áo Thun Nam',
        brand: 'Taca',
        price: { sale: 150000, currency: 'VND' },
        attributes: { material: 'cotton' },
        media: [{ url: 'http://cdn.com/img1.jpg' }],
      },
    };

    const currentDoc: ProductSearchDocument = {
      product_id: 'prod-1',
      shop_id: 'shop-1',
      slug: 'ao-thun-nam',
      title: 'Áo Thun Nam',
      description: 'Mô tả',
      category_ids: ['cat-1'],
      category_path: ['thoi-trang'],
      brand: 'Taca',
      price: 150000,
      currency: 'VND',
      rating_avg: 4.5,
      rating_count: 10,
      attributes: {},
      visibility_status: VisibilityStatus.PUBLISHED,
      source_version: 2,
      source_event_id: 'evt-100',
      indexed_at: '2026-10-14T10:00:00.000Z',
    };

    it('skips duplicate event by event_id', () => {
      const res = builder.build(baseEvent, currentDoc);
      expect(res).toEqual({ kind: 'SKIP', reason: 'DUPLICATE' });
    });

    it('skips older or equal version', () => {
      const olderEvent = {
        ...baseEvent,
        event_id: 'evt-101',
        version: 2,
      };
      const res = builder.build(olderEvent, currentDoc);
      expect(res).toEqual({ kind: 'SKIP', reason: 'OLD_VERSION' });
    });

    it('creates product with UPSERT and HIDDEN status', () => {
      const res = builder.build(baseEvent, null);
      expect(res.kind).toBe('UPSERT');
      if (res.kind === 'UPSERT') {
        expect(res.doc.visibility_status).toBe(VisibilityStatus.HIDDEN);
        expect(res.doc.price).toBe(150000);
        expect(res.doc.currency).toBe('VND');
        expect(res.doc.cover_url).toBe('http://cdn.com/img1.jpg');
        expect(res.doc.spec_attributes).toEqual({ material: 'cotton' });
      }
    });

    it('builds published product with UPSERT, spec_attributes, and suggests', () => {
      const pubEvent: DomainEvent = {
        ...baseEvent,
        event_id: 'evt-102',
        event_type: 'product.published',
        version: 3,
        payload: {
          product_id: 'prod-1',
          title: 'Áo Thun Nam Cao Cấp',
          brand: 'Taca Pro',
          price: { sale: 200000, currency: 'VND' },
          attributes: { material: 'silk' },
          media: [{ url: 'http://cdn.com/img2.jpg' }],
          visibility_status: VisibilityStatus.PUBLISHED,
        },
      };

      const res = builder.build(pubEvent, currentDoc);
      expect(res.kind).toBe('UPSERT');
      if (res.kind === 'UPSERT') {
        expect(res.doc.visibility_status).toBe(VisibilityStatus.PUBLISHED);
        expect(res.doc.price).toBe(200000);
        expect(res.doc.spec_attributes).toEqual({ material: 'silk' });
        expect(res.doc.cover_url).toBe('http://cdn.com/img2.jpg');
      }
    });

    it('builds updated product with PATCH changed fields', () => {
      const updateEvent: DomainEvent = {
        ...baseEvent,
        event_id: 'evt-103',
        event_type: 'product.updated',
        version: 3,
        payload: {
          product_id: 'prod-1',
          title: 'Áo Thun Mới',
          price: { sale: 180000 },
        },
      };

      const res = builder.build(updateEvent, currentDoc);
      expect(res.kind).toBe('PATCH');
      if (res.kind === 'PATCH') {
        expect(res.doc.title).toBe('Áo Thun Mới');
        expect(res.doc.price).toBe(180000);
      }
    });

    it('builds unblocked product with next_status ACTIVE => PUBLISHED and INACTIVE => HIDDEN', () => {
      const unblockActive: DomainEvent = {
        ...baseEvent,
        event_id: 'evt-107',
        event_type: 'product.unblocked',
        version: 3,
        payload: { product_id: 'prod-1', next_status: 'ACTIVE' },
      };
      const resActive = builder.build(unblockActive, currentDoc);
      expect(resActive.kind).toBe('PATCH');
      if (resActive.kind === 'PATCH') {
        expect(resActive.doc.visibility_status).toBe(
          VisibilityStatus.PUBLISHED,
        );
      }

      const unblockInactive: DomainEvent = {
        ...baseEvent,
        event_id: 'evt-108',
        event_type: 'product.unblocked',
        version: 4,
        payload: { product_id: 'prod-1', next_status: 'INACTIVE' },
      };
      const resInactive = builder.build(unblockInactive, {
        ...currentDoc,
        source_version: 3,
        source_event_id: 'evt-107',
      });
      expect(resInactive.kind).toBe('PATCH');
      if (resInactive.kind === 'PATCH') {
        expect(resInactive.doc.visibility_status).toBe(VisibilityStatus.HIDDEN);
      }
    });

    it('throws SEARCH_EVENT_INVALID for malformed event', () => {
      expect(() =>
        builder.build(null as unknown as DomainEvent, currentDoc),
      ).toThrow(SearchException);

      try {
        builder.build({} as unknown as DomainEvent, currentDoc);
      } catch (err: unknown) {
        expect(err).toBeInstanceOf(SearchException);
        expect((err as SearchException).code).toBe(
          SearchErrorCode.SEARCH_EVENT_INVALID,
        );
      }
    });
  });

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
      visibility_status: VisibilityStatus.PUBLISHED,
      source_version: 1,
      source_event_id: 'evt-1',
      indexed_at: '2026-10-14T10:00:00.000Z',
    };

    it('aggregates attributes and attribute_pairs from active SKUs and drops invalid keys', () => {
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
            'INVALID-KEY!': 'drop',
          },
        },
      };

      const res = aggregator.handleSkuEvent(baseProduct, sku1Event);
      expect(res).not.toBeNull();
      expect(res?.attributes).toEqual({
        color: ['Đỏ'],
        size: ['M'],
      });
      expect(res?.attribute_pairs).toEqual(['color=Đỏ', 'size=M']);
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
      expect(res?.attributes).toEqual({
        color: ['Xanh'],
        size: ['M'],
      });
      expect(res?.attribute_pairs).toEqual(['color=Xanh', 'size=M']);
    });
  });

  describe('EsIndexWriter', () => {
    let writer: EsIndexWriter;
    let clientMock: jest.Mocked<Partial<Client>>;

    beforeEach(() => {
      clientMock = {
        bulk: jest.fn(),
        mget: jest.fn(),
      };
      writer = new EsIndexWriter(clientMock as unknown as Client);
    });

    const createDummyDoc = (
      id: string,
      version: number,
    ): ProductSearchDocument => ({
      product_id: id,
      shop_id: 'shop-1',
      slug: `slug-${id}`,
      title: `Title ${id}`,
      description: 'desc',
      category_ids: ['cat-1'],
      category_path: ['path'],
      brand: 'Brand',
      price: 100000,
      currency: 'VND',
      rating_avg: 0,
      rating_count: 0,
      attributes: {},
      visibility_status: VisibilityStatus.PUBLISHED,
      source_version: version,
      source_event_id: `evt-${id}`,
      indexed_at: '2026-10-14T10:00:00.000Z',
    });

    it('chunks 1201 docs into 3 bulk calls (500, 500, 201) and treats 409 as skipped', async () => {
      const docs: ProductSearchDocument[] = [];
      for (let i = 1; i <= 1201; i++) {
        docs.push(createDummyDoc(`prod-${i}`, i));
      }

      (clientMock.bulk as jest.Mock).mockResolvedValue({
        errors: true,
        items: [
          { index: { status: 200 } },
          {
            index: {
              status: 409,
              error: { type: 'version_conflict_engine_exception' },
            },
          },
        ],
      });

      const summary = await writer.bulkUpsertWithSummary('products-read', docs);
      expect(clientMock.bulk).toHaveBeenCalledTimes(3);
      expect(summary.skipped).toBeGreaterThanOrEqual(1);
    });
  });

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

      (categoryProjectionMock.resolvePathIds as jest.Mock).mockResolvedValue([
        'cat-new-parent',
        'cat-child',
      ]);

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
});
