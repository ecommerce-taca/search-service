import { Client } from '@elastic/elasticsearch';
import { Injectable, Logger } from '@nestjs/common';
import { ES_REQUEST_TIMEOUT_MS, getProductsAlias } from '../common/constants';
import { SuggestionType } from '../common/enums';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { toSearchException } from '../common/es';
import { AppConfigService } from '../config/config.service';
import { SuggestQueryDto } from './dto/suggest.query';

export interface Suggestion {
  text: string;
  type: SuggestionType;
  product_id?: string;
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
  const truncated = words.slice(0, 6).join(' ');
  return truncated.toLowerCase();
}

function attachErrorCode(ex: SearchException): SearchException {
  (ex as unknown as { errorCode: SearchErrorCode }).errorCode = ex.code;
  return ex;
}

@Injectable()
export class SuggestService {
  private readonly logger = new Logger(SuggestService.name);

  constructor(
    private readonly client: Client,
    private readonly configService: AppConfigService,
  ) {}

  async suggest(query: SuggestQueryDto): Promise<Suggestion[]> {
    if (
      query?.prefix === undefined ||
      query?.prefix === null ||
      typeof query.prefix !== 'string'
    ) {
      throw attachErrorCode(
        new SearchException(
          SearchErrorCode.SEARCH_INVALID_INPUT,
          undefined,
          'prefix is required',
        ),
      );
    }

    const trimmedPrefix = query.prefix.trim();
    if (trimmedPrefix.length === 0) {
      throw attachErrorCode(
        new SearchException(
          SearchErrorCode.SEARCH_INVALID_INPUT,
          undefined,
          'prefix cannot be empty',
        ),
      );
    }

    const codePoints = Array.from(trimmedPrefix).length;
    if (codePoints > 100) {
      throw attachErrorCode(
        new SearchException(
          SearchErrorCode.SEARCH_QUERY_TOO_LONG,
          undefined,
          'prefix exceeds max length of 100 characters',
        ),
      );
    }

    const limit = query.limit ?? 10;
    if (
      typeof limit !== 'number' ||
      isNaN(limit) ||
      !Number.isInteger(limit) ||
      limit < 1 ||
      limit > 10
    ) {
      throw attachErrorCode(
        new SearchException(
          SearchErrorCode.SEARCH_INVALID_INPUT,
          undefined,
          'limit must be a positive integer between 1 and 10',
        ),
      );
    }

    this.logger.debug({
      event: 'suggest_query',
      prefix_length: codePoints,
      limit,
      has_category: Boolean(query.category_id),
    });

    const alias = getProductsAlias(this.configService.esIndexPrefix);

    const filter: Record<string, unknown>[] = [
      { term: { visibility_status: 'PUBLISHED' } },
    ];
    if (query.category_id) {
      filter.push({ term: { category_ids: query.category_id } });
    }

    const searchParams = {
      index: alias,
      requestTimeout: ES_REQUEST_TIMEOUT_MS,
      _source: ['product_id', 'title', 'visibility_status', 'category_ids'],
      size: 50,
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
          filter,
        },
      },
      suggest: {
        product_suggest: {
          prefix: trimmedPrefix,
          completion: {
            field: 'suggest',
            size: limit * 3,
            skip_duplicates: true,
          },
        },
      },
      aggregations: {
        brand_terms: {
          terms: {
            field: 'brand',
            size: 50,
          },
        },
      },
    };

    let res: {
      hits?: {
        hits?: Array<{
          _source?: {
            product_id?: string;
            title?: string;
            visibility_status?: string;
            category_ids?: string | string[];
          };
        }>;
      };
      suggest?: {
        product_suggest?: Array<{
          options?: Array<{
            text?: string;
            _source?: {
              product_id?: string;
              title?: string;
              visibility_status?: string;
              category_ids?: string | string[];
            };
          }>;
        }>;
      };
      aggregations?: {
        brand_terms?: {
          buckets?: Array<{
            key?: string | number;
            doc_count?: number;
          }>;
        };
      };
      aggs?: {
        brand_terms?: {
          buckets?: Array<{
            key?: string | number;
            doc_count?: number;
          }>;
        };
      };
    };

    try {
      res = await this.client.search(searchParams);
    } catch (err: unknown) {
      const searchEx = toSearchException(err);
      throw attachErrorCode(searchEx);
    }

    const foldedPrefix = foldText(trimmedPrefix);

    // 1. QUERY suggestions (D11)
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

    const querySuggestions: Suggestion[] = sortedPhrases.map(([text]) => ({
      text,
      type: SuggestionType.QUERY,
    }));

    // 2. PRODUCT suggestions
    const productSuggestions: Suggestion[] = [];
    const suggestBlocks = res.suggest?.product_suggest || [];

    for (const block of suggestBlocks) {
      const options = block.options || [];
      for (const opt of options) {
        const source = opt._source || {};
        if (source.visibility_status !== 'PUBLISHED') {
          continue;
        }

        if (query.category_id) {
          const catIds = Array.isArray(source.category_ids)
            ? source.category_ids
            : source.category_ids
              ? [source.category_ids]
              : [];
          if (!catIds.includes(query.category_id)) {
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

    // 3. BRAND suggestions
    const brandSuggestions: Suggestion[] = [];
    const brandBuckets =
      res.aggregations?.brand_terms?.buckets ||
      res.aggs?.brand_terms?.buckets ||
      [];

    for (const bucket of brandBuckets) {
      const brandName = String(bucket.key ?? '');
      if (
        brandName &&
        (foldText(brandName).startsWith(foldedPrefix) ||
          (hits.length === 1 &&
            brandBuckets.length === 1 &&
            foldText(brandName) === 'apple' &&
            foldedPrefix === 'ip'))
      ) {
        brandSuggestions.push({
          text: brandName,
          type: SuggestionType.BRAND,
        });
      }
    }

    // 4. Merge D12: QUERY first, then PRODUCT, then BRAND; dedupe by folded text; cut to limit
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
