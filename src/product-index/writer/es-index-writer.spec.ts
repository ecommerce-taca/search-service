import { Client } from '@elastic/elasticsearch';
import { ProductSearchDocument } from '../interfaces';
import { EsIndexWriter } from './es-index-writer';

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

  const createDummyDoc = (id: string, version: number): ProductSearchDocument => ({
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
    visibility_status: 'PUBLISHED' as any,
    source_version: version,
    source_event_id: `evt-${id}`,
    indexed_at: '2026-10-14T10:00:00.000Z',
  });

  it('chunks 1201 docs into 3 bulk calls (500, 500, 201)', async () => {
    const docs: ProductSearchDocument[] = [];
    for (let i = 1; i <= 1201; i++) {
      docs.push(createDummyDoc(`prod-${i}`, i));
    }

    (clientMock.bulk as jest.Mock).mockResolvedValue({
      errors: false,
      items: [
        { index: { status: 200 } },
      ],
    });

    await writer.bulkUpsert('products-read', docs);

    expect(clientMock.bulk).toHaveBeenCalledTimes(3);

    const call1Ops = (clientMock.bulk as jest.Mock).mock.calls[0][0].operations;
    const call2Ops = (clientMock.bulk as jest.Mock).mock.calls[1][0].operations;
    const call3Ops = (clientMock.bulk as jest.Mock).mock.calls[2][0].operations;

    // Each doc has 2 operations: action descriptor + document body
    expect(call1Ops).toHaveLength(500 * 2);
    expect(call2Ops).toHaveLength(500 * 2);
    expect(call3Ops).toHaveLength(201 * 2);

    // Verify external_gte versioning is used on index operations
    expect(call1Ops[0]).toEqual({
      index: {
        _index: 'products-read',
        _id: 'prod-1',
        version: 1,
        version_type: 'external_gte',
      },
    });
  });

  it('treats 409 version conflict as skipped in bulkUpsertWithSummary', async () => {
    const docs = [
      createDummyDoc('prod-1', 1),
      createDummyDoc('prod-2', 2),
    ];

    (clientMock.bulk as jest.Mock).mockResolvedValue({
      errors: true,
      items: [
        { index: { status: 200 } },
        {
          index: {
            status: 409,
            error: {
              type: 'version_conflict_engine_exception',
              reason: 'version conflict, document version [2] is higher than current [1]',
            },
          },
        },
      ],
    });

    const summary = await writer.bulkUpsertWithSummary('products-read', docs);
    expect(summary).toEqual({
      applied: 1,
      skipped: 1,
      failed: 0,
    });
  });

  it('executes bulkPartialUpdate with update descriptors', async () => {
    const updates = [
      { id: 'prod-1', doc: { title: 'Updated Title' } },
    ];

    (clientMock.bulk as jest.Mock).mockResolvedValue({
      errors: false,
      items: [{ update: { status: 200 } }],
    });

    await writer.bulkPartialUpdate('products-read', updates);

    expect(clientMock.bulk).toHaveBeenCalledTimes(1);
    const ops = (clientMock.bulk as jest.Mock).mock.calls[0][0].operations;
    expect(ops[0]).toEqual({
      update: {
        _index: 'products-read',
        _id: 'prod-1',
        retry_on_conflict: 3,
      },
    });
    expect(ops[1]).toEqual({
      doc: { title: 'Updated Title' },
    });
  });

  it('retrieves current versions with mget', async () => {
    (clientMock.mget as jest.Mock).mockResolvedValue({
      docs: [
        {
          _id: 'prod-1',
          found: true,
          _source: { source_version: 10 },
        },
        {
          _id: 'prod-2',
          found: false,
        },
      ],
    });

    const versions = await writer.getCurrentVersions('products-read', ['prod-1', 'prod-2']);
    expect(versions.size).toBe(1);
    expect(versions.get('prod-1')).toBe(10);
    expect(versions.has('prod-2')).toBe(false);
  });
});
