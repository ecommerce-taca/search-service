import { readFileSync } from 'fs';
import { join } from 'path';

export type FixtureRecord = Record<string, unknown> & {
  aggregate_id?: string;
  event_id?: string;
  product_id?: string;
  category_id?: string;
  sku_id?: string;
  [key: string]: unknown;
};

function loadFixture<T = unknown>(fileName: string): T {
  const filePath = join(__dirname, fileName);
  return JSON.parse(readFileSync(filePath, 'utf-8')) as T;
}

const categoriesJson = loadFixture<FixtureRecord[]>('categories.json');
const productsJson = loadFixture<FixtureRecord[]>('products.json');
const skusJson = loadFixture<FixtureRecord[]>('skus.json');
const ratingsJson = loadFixture<FixtureRecord[]>('ratings.json');
const orderingDuplicatesJson = loadFixture<FixtureRecord[]>(
  'ordering-duplicates.json',
);
const poisonEventsJson = loadFixture<FixtureRecord[]>('poison-events.json');

export const INDEXING_FIXTURES = {
  categories: categoriesJson,
  products: productsJson,
  skus: skusJson,
  ratings: ratingsJson,
  orderingDuplicates: orderingDuplicatesJson,
  poisonEvents: poisonEventsJson,
};

export {
  categoriesJson,
  productsJson,
  skusJson,
  ratingsJson,
  orderingDuplicatesJson,
  poisonEventsJson,
};
