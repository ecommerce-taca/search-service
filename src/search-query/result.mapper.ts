import { estypes } from '@elastic/elasticsearch';
import { Injectable } from '@nestjs/common';
import { ProductSearchDocument } from '../product-index/interfaces';
import { ParsedSearchQuery } from './query-parser';

export interface ProductCardShop {
  shop_id: string;
  name: string;
}

export interface ProductCardPrice {
  amount: number;
  currency: string;
}

export interface ProductCard {
  product_id: string;
  title: string;
  slug: string;
  shop: ProductCardShop;
  price: ProductCardPrice;
  cover_url?: string;
  rating_avg: number;
}

export interface CategoryFacetItem {
  category_id: string;
  name: string;
  count: number;
}

export interface BrandFacetItem {
  value: string;
  count: number;
}

export interface PriceFacetBucket {
  min: number;
  max: number | null;
  count: number;
}

export interface AttributeFacetItem {
  value: string;
  count: number;
}

export interface SearchFacets {
  category: CategoryFacetItem[];
  brand: BrandFacetItem[];
  price: PriceFacetBucket[];
  attributes: Record<string, AttributeFacetItem[]>;
}

export interface SearchResultMeta {
  page: number;
  size: number;
  total: number;
  total_pages: number;
  index_as_of: string | null;
  facets?: SearchFacets;
  [key: string]: unknown;
}

export interface SearchResult {
  data: ProductCard[];
  meta: SearchResultMeta;
}

@Injectable()
export class SearchResultMapper {
  mapCard(
    doc: Partial<ProductSearchDocument> | Record<string, unknown>,
  ): ProductCard {
    return SearchResultMapper.mapCard(doc);
  }

  static mapCard(
    doc: Partial<ProductSearchDocument> | Record<string, unknown>,
  ): ProductCard {
    const d = doc as Partial<ProductSearchDocument> & Record<string, unknown>;
    const rawShop = d.shop;
    const shopId = rawShop?.shop_id || (d['shop_id'] as string) || '';
    const shopName = rawShop?.name || (d['shop_name'] as string) || '';

    const card: ProductCard = {
      product_id: String(d.product_id || ''),
      title: String(d.title || ''),
      slug: String(d.slug || ''),
      shop: {
        shop_id: shopId,
        name: shopName,
      },
      price: {
        amount: typeof d.price === 'number' ? d.price : 0,
        currency: (d.currency as string) || 'VND',
      },
      rating_avg: typeof d.rating_avg === 'number' ? d.rating_avg : 0,
    };

    if (d.cover_url) {
      card.cover_url = String(d.cover_url);
    }

    return card;
  }

  map(
    response: estypes.SearchResponse<ProductSearchDocument>,
    parsed: ParsedSearchQuery,
    facets?: SearchFacets,
    indexAsOf?: string | null,
  ): SearchResult {
    return SearchResultMapper.map(response, parsed, facets, indexAsOf);
  }

  static map(
    response: estypes.SearchResponse<ProductSearchDocument>,
    parsed: ParsedSearchQuery,
    facets?: SearchFacets,
    indexAsOf?: string | null,
  ): SearchResult {
    const hits = response.hits?.hits || [];
    const data: ProductCard[] = hits.map((hit) =>
      SearchResultMapper.mapCard(hit._source || {}),
    );

    const rawTotal = response.hits?.total;
    const total =
      typeof rawTotal === 'number'
        ? rawTotal
        : typeof rawTotal?.value === 'number'
          ? rawTotal.value
          : 0;

    const totalPages = total === 0 ? 0 : Math.ceil(total / parsed.size);

    const meta: SearchResultMeta = {
      page: parsed.page,
      size: parsed.size,
      total,
      total_pages: totalPages,
      index_as_of: indexAsOf ?? null,
    };

    // D7: include_facets = false -> meta.facets key omitted
    if (parsed.includeFacets && facets !== undefined) {
      meta.facets = facets;
    }

    return { data, meta };
  }
}

export const ResultMapper = SearchResultMapper;
export type ResultMapper = SearchResultMapper;
