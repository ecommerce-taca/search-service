import { normalizeExtendedJson } from './extended-json';

describe('normalizeExtendedJson', () => {
  it('converts extended json numbers to regular numbers in nested structures', () => {
    const input = {
      price: {
        base: { $numberInt: '2683000' },
        sale: { $numberLong: '2409000' },
        currency: 'VND',
      },
      ratio: { $numberDouble: '0.5' },
      nested: [{ n: { $numberDecimal: '12.5' } }],
    };

    expect(normalizeExtendedJson(input)).toEqual({
      price: {
        base: 2683000,
        sale: 2409000,
        currency: 'VND',
      },
      ratio: 0.5,
      nested: [{ n: 12.5 }],
    });
  });

  it('normalizes $date and $oid representations', () => {
    const isoString = '2026-10-15T01:44:30.000Z';
    const timestamp = 1792026816506;
    const input = {
      dateStr: { $date: isoString },
      dateLong: { $date: { $numberLong: '1792026816506' } },
      oid: { $oid: 'abc' },
    };

    expect(normalizeExtendedJson(input)).toEqual({
      dateStr: new Date(isoString).toISOString(),
      dateLong: new Date(timestamp).toISOString(),
      oid: 'abc',
    });
  });

  it('preserves standard JSON structures as-is', () => {
    const input = {
      id: 'prod-123',
      name: 'Test Product',
      tags: ['electronics', 'audio'],
      count: 42,
      price: 99.99,
      active: true,
      metadata: null,
    };

    expect(normalizeExtendedJson(input)).toEqual(input);
  });
});
