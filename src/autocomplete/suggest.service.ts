import { Client, estypes } from '@elastic/elasticsearch';
import { Injectable, Logger } from '@nestjs/common';
import { ES_REQUEST_TIMEOUT_MS, getProductsAlias } from '../common/constants';
import { SuggestionType } from '../common/enums';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { toSearchException } from '../common/es';
import { AppConfigService } from '../config/config.service';
import { SuggestQueryDto } from './dto/suggest.query';

// Named constants replacing magic numbers (MAJOR 3)
const QUERY_FETCH_SIZE = 50;
const COMPLETION_SIZE_MULTIPLIER = 3;
const PHRASE_MAX_WORDS = 6;
const PREFIX_MAX_CODE_POINTS = 100;
const LIMIT_MIN = 1;
const LIMIT_MAX = 10;
const DEFAULT_LIMIT = 10;

export interface Suggestion {
  text: string;
  type: SuggestionType;
  product_id?: string;
}

export interface ProductDocSource {
  product_id?: string;
  title?: string;
  visibility_status?: string;
  category_ids?: string | string[];
}

export function foldText(str: string): string {
  return str
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/đ/g, 'd')
    .replace(/Đ/g, 'd')
    .toLowerCase();
}

export function extractPhrase(
  title: string,
  foldedPrefix: string,
): string | null {
  const foldedTitle = foldText(title);
  let pos = -1;
  let idx = foldedTitle.indexOf(foldedPrefix);
  while (idx !== -1) {
    if (idx === 0 || /\s/.test(foldedTitle[idx - 1])) {
      pos = idx;
      break;
    }
    idx = foldedTitle.indexOf(foldedPrefix, idx + 1);
  }
  if (pos === -1) {
    pos = foldedTitle.indexOf(foldedPrefix);
  }
  if (pos === -1) {
    return null;
  }

  const remaining = title.slice(pos).trim();
  const words = remaining.split(/\s+/).filter(Boolean);
  const truncated = words.slice(0, PHRASE_MAX_WORDS).join(' ');
  return truncated.toLowerCase();
}

@Injectable()
export class SuggestService {
  private readonly logger = new Logger(SuggestService.name);

  constructor(
    private readonly client: Client,
    private readonly configService: AppConfigService,
  ) {}

  async suggest(query: SuggestQueryDto): Promise<Suggestion[]> {
    const { trimmedPrefix, limit, codePoints } = this.validate(query);

    this.logger.debug({
      event: 'suggest_query',
      prefix_length: codePoints,
      limit,
      has_category: Boolean(query.category_id),
    });

    const searchRequest = this.buildRequest(
      trimmedPrefix,
      limit,
      query.category_id,
    );

    let res: estypes.SearchResponse<ProductDocSource>;
    try {
      // requestTimeout is a transport option: inside the request object the
      // client would send it as an (invalid) body key and Elasticsearch
      // would reject the search with a 400.
      res = await this.client.search<ProductDocSource>(searchRequest, {
        requestTimeout: ES_REQUEST_TIMEOUT_MS,
      });
    } catch (err: unknown) {
      throw toSearchException(err);
    }

    const foldedPrefix = foldText(trimmedPrefix);
    const querySuggestions = this.toQuerySuggestions(res, foldedPrefix);
    const productSuggestions = this.toProductSuggestions(
      res,
      query.category_id,
    );
    const brandSuggestions = this.toBrandSuggestions(res, foldedPrefix);

    return this.merge(
      querySuggestions,
      productSuggestions,
      brandSuggestions,
      limit,
    );
  }

  private validate(query: SuggestQueryDto): {
    trimmedPrefix: string;
    limit: number;
    codePoints: number;
  } {
    if (
      query.prefix === undefined ||
      query.prefix === null ||
      typeof query.prefix !== 'string'
    ) {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        undefined,
        "Query parameter 'prefix' is required.",
      );
    }

    const trimmedPrefix = query.prefix.trim();
    if (trimmedPrefix.length === 0) {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        undefined,
        "Query parameter 'prefix' cannot be empty.",
      );
    }

    const codePoints = Array.from(trimmedPrefix).length;
    if (codePoints > PREFIX_MAX_CODE_POINTS) {
      throw new SearchException(
        SearchErrorCode.SEARCH_QUERY_TOO_LONG,
        undefined,
        "Query parameter 'prefix' exceeds 100 characters.",
      );
    }

    const limit = query.limit ?? DEFAULT_LIMIT;
    if (
      typeof limit !== 'number' ||
      Number.isNaN(limit) ||
      !Number.isInteger(limit) ||
      limit < LIMIT_MIN ||
      limit > LIMIT_MAX
    ) {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        undefined,
        limit > LIMIT_MAX
          ? 'Limit cannot exceed 10.'
          : 'limit must be a positive integer between 1 and 10',
      );
    }

    return { trimmedPrefix, limit, codePoints };
  }

  private buildRequest(
    trimmedPrefix: string,
    limit: number,
    categoryId?: string,
  ): estypes.SearchRequest {
    const alias = getProductsAlias(this.configService.esIndexPrefix);

    // Build the base filter once and reuse (MAJOR 2)
    const baseFilter: estypes.QueryDslQueryContainer[] = [
      { term: { visibility_status: 'PUBLISHED' } },
    ];
    if (categoryId) {
      baseFilter.push({ term: { category_ids: categoryId } });
    }

    // Brand filter = base filter + prefix filter (MAJOR 2)
    const brandFilter: estypes.QueryDslQueryContainer[] = [
      ...baseFilter,
      {
        prefix: {
          brand: {
            value: trimmedPrefix,
            case_insensitive: true,
          },
        },
      },
    ];

    return {
      index: alias,
      _source: ['product_id', 'title', 'visibility_status', 'category_ids'],
      size: QUERY_FETCH_SIZE,
      query: {
        bool: {
          must: [
            {
              match_phrase_prefix: {
                title: {
                  query: trimmedPrefix,
                },
              },
            },
          ],
          filter: baseFilter,
        },
      },
      suggest: {
        product_suggest: {
          prefix: trimmedPrefix,
          completion: {
            field: 'suggest',
            size: limit * COMPLETION_SIZE_MULTIPLIER,
            skip_duplicates: true,
          },
        },
      },
      aggregations: {
        brands_global: {
          global: {},
          aggregations: {
            brands_filtered: {
              filter: {
                bool: {
                  filter: brandFilter,
                },
              },
              aggregations: {
                brand_terms: {
                  terms: {
                    field: 'brand',
                    size: limit,
                  },
                },
              },
            },
          },
        },
      },
    };
  }

  private toQuerySuggestions(
    res: estypes.SearchResponse<ProductDocSource>,
    foldedPrefix: string,
  ): Suggestion[] {
    const hits = res.hits?.hits || [];
    const phraseFrequency = new Map<string, number>();

    for (const hit of hits) {
      const title = hit._source?.title;
      if (typeof title === 'string' && title.length > 0) {
        const phrase = extractPhrase(title, foldedPrefix);
        if (phrase) {
          phraseFrequency.set(phrase, (phraseFrequency.get(phrase) || 0) + 1);
        }
      }
    }

    const sortedPhrases = Array.from(phraseFrequency.entries()).sort((a, b) => {
      if (b[1] !== a[1]) {
        return b[1] - a[1];
      }
      return a[0].localeCompare(b[0], 'vi');
    });

    return sortedPhrases.map(([text]) => ({
      text,
      type: SuggestionType.QUERY,
    }));
  }

  private toProductSuggestions(
    res: estypes.SearchResponse<ProductDocSource>,
    categoryId?: string,
  ): Suggestion[] {
    const productSuggestions: Suggestion[] = [];
    const suggestBlocks = (res.suggest?.product_suggest || []) as Array<
      estypes.SearchCompletionSuggest<ProductDocSource>
    >;

    for (const block of suggestBlocks) {
      const rawOptions = block.options || [];
      const options = Array.isArray(rawOptions) ? rawOptions : [rawOptions];
      for (const opt of options) {
        const source = (opt._source || {}) as ProductDocSource;
        if (source.visibility_status !== 'PUBLISHED') {
          continue;
        }

        if (categoryId) {
          const catIds = Array.isArray(source.category_ids)
            ? source.category_ids
            : source.category_ids
              ? [source.category_ids]
              : [];
          if (!catIds.includes(categoryId)) {
            continue;
          }
        }

        const text = opt.text || source.title;
        if (text) {
          productSuggestions.push({
            text,
            type: SuggestionType.PRODUCT,
            product_id: source.product_id,
          });
        }
      }
    }

    return productSuggestions;
  }

  private toBrandSuggestions(
    res: estypes.SearchResponse<ProductDocSource>,
    foldedPrefix: string,
  ): Suggestion[] {
    const brandSuggestions: Suggestion[] = [];
    const aggs = res.aggregations;
    if (!aggs) {
      return brandSuggestions;
    }

    // Read only from brands_global.brands_filtered.brand_terms (MAJOR 1)
    let buckets: Array<{ key?: string | number; doc_count?: number }> = [];

    const globalAgg = aggs.brands_global as
      | (estypes.AggregationsGlobalAggregate & Record<string, unknown>)
      | undefined;
    if (globalAgg?.brands_filtered) {
      const filterAgg = globalAgg.brands_filtered as
        | (estypes.AggregationsFilterAggregate & Record<string, unknown>)
        | undefined;
      const termsAgg = filterAgg?.brand_terms as
        estypes.AggregationsStringTermsAggregate | undefined;
      if (Array.isArray(termsAgg?.buckets)) {
        buckets = termsAgg.buckets;
      }
    }

    for (const bucket of buckets) {
      const brandName = String(bucket.key ?? '');
      if (brandName && foldText(brandName).startsWith(foldedPrefix)) {
        brandSuggestions.push({
          text: brandName,
          type: SuggestionType.BRAND,
        });
      }
    }

    return brandSuggestions;
  }

  private merge(
    querySuggestions: Suggestion[],
    productSuggestions: Suggestion[],
    brandSuggestions: Suggestion[],
    limit: number,
  ): Suggestion[] {
    const combined: Suggestion[] = [
      ...querySuggestions,
      ...productSuggestions,
      ...brandSuggestions,
    ];

    const seenFolded = new Set<string>();
    const finalSuggestions: Suggestion[] = [];

    for (const item of combined) {
      const folded = foldText(item.text.trim());
      if (!seenFolded.has(folded)) {
        seenFolded.add(folded);
        finalSuggestions.push(item);
        if (finalSuggestions.length >= limit) {
          break;
        }
      }
    }

    return finalSuggestions;
  }
}
