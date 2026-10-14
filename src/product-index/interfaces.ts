import { DomainEvent } from '../event-consumer/interfaces';

export interface ProductSearchDocument {
  product_id: string;
  shop_id: string;
  slug: string;
  title: string;
  title_exact?: string;
  description: string;
  category_ids: string[];
  category_path: string[];
  brand: string;
  price: number;
  currency: string;
  rating_avg: number;
  rating_count: number;
  sales_count: number;
  attributes: Record<string, unknown>;
  suggest?: string[] | { input: string[]; weight?: number };
  visibility_status: string;
  source_version: number;
  source_event_id: string;
  indexed_at: string;
  created_at?: string;
  cover_url?: string;
  shop?: {
    shop_id: string;
    name: string;
  };
}

export interface IProductDocumentBuilder {
  fromProductEvent(
    evt: DomainEvent<unknown>,
    current?: ProductSearchDocument,
  ): ProductSearchDocument;
}

export interface IIndexWriter {
  bulkUpsert(index: string, docs: ProductSearchDocument[]): Promise<void>;
  bulkPartialUpdate(
    index: string,
    updates: Array<{ id: string; doc: Partial<ProductSearchDocument> }>,
  ): Promise<void>;
  getCurrentVersions(
    index: string,
    ids: string[],
  ): Promise<Map<string, number>>;
}

export const PRODUCT_DOCUMENT_BUILDER = 'IProductDocumentBuilder';
export const PRODUCT_DOCUMENT_BUILDER_TOKEN = PRODUCT_DOCUMENT_BUILDER;

export const INDEX_WRITER = 'IIndexWriter';
export const INDEX_WRITER_TOKEN = INDEX_WRITER;
