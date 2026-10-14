import { VisibilityStatus } from '../common/enums';
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
  attributes: Record<string, string[]>;
  spec_attributes?: Record<string, string[] | string>;
  attribute_pairs?: string[];
  variant_skus?: Record<
    string,
    {
      status: string;
      attributes: Record<string, string>;
      version: number;
    }
  >;
  suggest?: string[] | { input: string[]; weight?: number };
  visibility_status: VisibilityStatus;
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

export type DocumentChange =
  | { kind: 'UPSERT'; id: string; doc: ProductSearchDocument }
  | {
      kind: 'PATCH';
      id: string;
      doc: Partial<ProductSearchDocument>;
      sourceVersion: number;
      sourceEventId: string;
    }
  | {
      kind: 'SKIP';
      reason: 'OLD_VERSION' | 'DUPLICATE' | 'NOT_APPLICABLE';
    };

export interface IProductDocumentBuilder {
  build(
    evt: DomainEvent<unknown>,
    current: ProductSearchDocument | null,
  ): DocumentChange;
}

export interface CategoryProjection {
  category_id: string;
  name: string;
  parent_id?: string | null;
  path_ids: string[];
  status: string;
  source_version: number;
  updated_at: string;
}

export interface ICategoryProjection {
  getMany(ids: string[]): Promise<Map<string, CategoryProjection>>;
  resolvePathIds(categoryId: string): Promise<string[]>;
}

export const CATEGORY_PROJECTION = 'ICategoryProjection';

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

export const INDEX_WRITER = 'IIndexWriter';
