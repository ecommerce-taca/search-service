import { VisibilityStatus } from '../../common/enums';
import { SearchErrorCode } from '../../common/errors/error-codes';
import { SearchException } from '../../common/errors/search.exception';
import { DomainEvent } from '../../event-consumer/interfaces';
import { ProductSearchDocument } from '../interfaces';
import { ProductDocumentBuilder } from './product-document.builder';

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

  it('builds published product with UPSERT and PUBLISHED status and spec_attributes', () => {
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
      expect(res.doc.suggest).toEqual({
        input: ['Áo Thun Nam Cao Cấp', 'Taca Pro'],
      });
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
      expect(res.sourceVersion).toBe(3);
    }
  });

  it('builds unpublished product with PATCH HIDDEN', () => {
    const unpubEvent: DomainEvent = {
      ...baseEvent,
      event_id: 'evt-104',
      event_type: 'product.unpublished',
      version: 3,
    };

    const res = builder.build(unpubEvent, currentDoc);
    expect(res).toEqual({
      kind: 'PATCH',
      id: 'prod-1',
      doc: {
        visibility_status: VisibilityStatus.HIDDEN,
        source_version: 3,
        source_event_id: 'evt-104',
        indexed_at: baseEvent.occurred_at,
      },
      sourceVersion: 3,
      sourceEventId: 'evt-104',
    });
  });

  it('builds archived product with PATCH DELETED', () => {
    const archiveEvent: DomainEvent = {
      ...baseEvent,
      event_id: 'evt-105',
      event_type: 'product.archived',
      version: 3,
    };

    const res = builder.build(archiveEvent, currentDoc);
    expect(res).toEqual({
      kind: 'PATCH',
      id: 'prod-1',
      doc: {
        visibility_status: VisibilityStatus.DELETED,
        source_version: 3,
        source_event_id: 'evt-105',
        indexed_at: baseEvent.occurred_at,
      },
      sourceVersion: 3,
      sourceEventId: 'evt-105',
    });
  });

  it('builds blocked product with PATCH HIDDEN', () => {
    const blockEvent: DomainEvent = {
      ...baseEvent,
      event_id: 'evt-106',
      event_type: 'product.blocked',
      version: 3,
    };

    const res = builder.build(blockEvent, currentDoc);
    expect(res).toEqual({
      kind: 'PATCH',
      id: 'prod-1',
      doc: {
        visibility_status: VisibilityStatus.HIDDEN,
        source_version: 3,
        source_event_id: 'evt-106',
        indexed_at: baseEvent.occurred_at,
      },
      sourceVersion: 3,
      sourceEventId: 'evt-106',
    });
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
      expect(resActive.doc.visibility_status).toBe(VisibilityStatus.PUBLISHED);
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

  it('skips unknown event types with NOT_APPLICABLE', () => {
    const unknownEvt: DomainEvent = {
      ...baseEvent,
      event_id: 'evt-109',
      event_type: 'unknown.event',
      version: 3,
    };
    const res = builder.build(unknownEvt, currentDoc);
    expect(res).toEqual({ kind: 'SKIP', reason: 'NOT_APPLICABLE' });
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

  it('withCategoryIds attaches category_ids to UPSERT and PATCH changes', () => {
    const upsertChange = builder.build(baseEvent, null);
    const updatedUpsert = builder.withCategoryIds(upsertChange, [
      'cat-1',
      'cat-2',
    ]);
    if (updatedUpsert.kind === 'UPSERT') {
      expect(updatedUpsert.doc.category_ids).toEqual(['cat-1', 'cat-2']);
    }
  });
});
