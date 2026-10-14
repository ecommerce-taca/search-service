import { Injectable } from '@nestjs/common';
import {
  PAGE_SIZE_DEFAULT,
  PAGE_SIZE_MAX,
  QUERY_MAX_LENGTH,
} from '../common/constants';
import { SortOption } from '../common/enums';
import {
  ERROR_MESSAGES_MAP,
  SearchErrorCode,
} from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { SearchProductsQueryDto } from './dto/search-products.query';

const ALLOWED_QUERY_FIELDS = new Set([
  'q',
  'category_id',
  'shop_id',
  'brand',
  'min_price',
  'max_price',
  'sort',
  'page',
  'size',
  'include_facets',
  'attribute',
]);

const DANGEROUS_KEYS = new Set(['__proto__', 'constructor', 'prototype']);
const ATTR_KEY_REGEX = /^[a-z0-9_]{1,64}$/i;

export interface ParsedSearchFilters {
  categoryId?: string;
  shopId?: string;
  brands: string[];
  minPrice?: number;
  maxPrice?: number;
  attributes: Record<string, string[]>;
}

export interface ParsedSearchQuery {
  text?: string;
  filters: ParsedSearchFilters;
  sort: SortOption;
  page: number;
  size: number;
  includeFacets: boolean;
}

@Injectable()
export class QueryParser {
  parse(dto: SearchProductsQueryDto): ParsedSearchQuery {
    return QueryParser.parse(dto);
  }

  static parse(
    dto: SearchProductsQueryDto | Record<string, unknown>,
  ): ParsedSearchQuery {
    if (!dto || typeof dto !== 'object' || Array.isArray(dto)) {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
        ['Invalid query object'],
      );
    }

    // Reject unknown root query parameters
    const unknownFields = Object.keys(dto).filter(
      (key) => !ALLOWED_QUERY_FIELDS.has(key),
    );
    if (unknownFields.length > 0) {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
        unknownFields.map((field) => `Unexpected parameter '${field}'`),
      );
    }

    // Validate q
    let text: string | undefined;
    if (dto.q !== undefined && dto.q !== null) {
      if (typeof dto.q !== 'string') {
        throw new SearchException(
          SearchErrorCode.SEARCH_INVALID_INPUT,
          ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
          ['q must be a string'],
        );
      }
      const codePointsLength = Array.from(dto.q).length;
      if (codePointsLength > QUERY_MAX_LENGTH) {
        throw new SearchException(
          SearchErrorCode.SEARCH_QUERY_TOO_LONG,
          ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_QUERY_TOO_LONG],
        );
      }
      const trimmed = dto.q.trim();
      if (trimmed.length > 0) {
        text = trimmed;
      }
    }

    // Validate category_id
    let categoryId: string | undefined;
    if (dto.category_id !== undefined && dto.category_id !== null) {
      if (typeof dto.category_id !== 'string') {
        throw new SearchException(
          SearchErrorCode.SEARCH_INVALID_INPUT,
          ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
          ['category_id must be a string'],
        );
      }
      const trimmed = dto.category_id.trim();
      if (trimmed.length > 0) {
        categoryId = trimmed;
      }
    }

    // Validate shop_id
    let shopId: string | undefined;
    if (dto.shop_id !== undefined && dto.shop_id !== null) {
      if (typeof dto.shop_id !== 'string') {
        throw new SearchException(
          SearchErrorCode.SEARCH_INVALID_INPUT,
          ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
          ['shop_id must be a string'],
        );
      }
      const trimmed = dto.shop_id.trim();
      if (trimmed.length > 0) {
        shopId = trimmed;
      }
    }

    // Validate brand
    const brands: string[] = [];
    if (
      dto.brand !== undefined &&
      dto.brand !== null &&
      (dto.brand as unknown) !== ''
    ) {
      const rawBrands = Array.isArray(dto.brand) ? dto.brand : [dto.brand];
      for (const item of rawBrands) {
        if (typeof item !== 'string') {
          throw new SearchException(
            SearchErrorCode.SEARCH_INVALID_INPUT,
            ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
            ['brand must be a string or array of strings'],
          );
        }
        const trimmed = item.trim();
        if (trimmed.length > 0) {
          brands.push(trimmed);
        }
      }
    }

    // Validate min_price & max_price
    let minPrice: number | undefined;
    let maxPrice: number | undefined;

    if (
      dto.min_price !== undefined &&
      dto.min_price !== null &&
      (dto.min_price as unknown) !== ''
    ) {
      const val = Number(dto.min_price);
      if (
        !Number.isInteger(val) ||
        isNaN(val) ||
        val < 0 ||
        val > 1_000_000_000_000
      ) {
        throw new SearchException(
          SearchErrorCode.SEARCH_INVALID_INPUT,
          ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
          ['min_price must be an integer between 0 and 10^12'],
        );
      }
      minPrice = val;
    }

    if (
      dto.max_price !== undefined &&
      dto.max_price !== null &&
      (dto.max_price as unknown) !== ''
    ) {
      const val = Number(dto.max_price);
      if (
        !Number.isInteger(val) ||
        isNaN(val) ||
        val < 0 ||
        val > 1_000_000_000_000
      ) {
        throw new SearchException(
          SearchErrorCode.SEARCH_INVALID_INPUT,
          ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
          ['max_price must be an integer between 0 and 10^12'],
        );
      }
      maxPrice = val;
    }

    if (
      minPrice !== undefined &&
      maxPrice !== undefined &&
      minPrice > maxPrice
    ) {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
        ['min_price cannot exceed max_price'],
      );
    }

    // Validate sort
    let sort: SortOption;
    if (
      dto.sort !== undefined &&
      dto.sort !== null &&
      (dto.sort as unknown) !== ''
    ) {
      if (!Object.values(SortOption).includes(dto.sort as SortOption)) {
        throw new SearchException(
          SearchErrorCode.SEARCH_INVALID_INPUT,
          ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
          [`Unsupported sort option '${dto.sort}'`],
        );
      }
      sort = dto.sort as SortOption;
    } else {
      // D6 default sort: relevance when q present, newest when q absent
      sort = text ? SortOption.RELEVANCE : SortOption.NEWEST;
    }

    // Validate page
    let page = 1;
    if (
      dto.page !== undefined &&
      dto.page !== null &&
      (dto.page as unknown) !== ''
    ) {
      const p = Number(dto.page);
      if (!Number.isInteger(p) || isNaN(p) || p < 1) {
        throw new SearchException(
          SearchErrorCode.SEARCH_INVALID_INPUT,
          ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
          ['page must be an integer >= 1'],
        );
      }
      page = p;
    }

    // Validate size
    let size = PAGE_SIZE_DEFAULT;
    if (
      dto.size !== undefined &&
      dto.size !== null &&
      (dto.size as unknown) !== ''
    ) {
      const s = Number(dto.size);
      if (!Number.isInteger(s) || isNaN(s) || s < 1 || s > PAGE_SIZE_MAX) {
        throw new SearchException(
          SearchErrorCode.SEARCH_INVALID_INPUT,
          ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
          ['size must be an integer between 1 and 100'],
        );
      }
      size = s;
    }

    // Max result window
    if (page * size > 10000) {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
        ['page * size cannot exceed 10000'],
      );
    }

    // Validate include_facets
    let includeFacets = true;
    if (
      dto.include_facets !== undefined &&
      dto.include_facets !== null &&
      (dto.include_facets as unknown) !== ''
    ) {
      if (typeof dto.include_facets === 'boolean') {
        includeFacets = dto.include_facets;
      } else if (
        (dto.include_facets as unknown) === 'true' ||
        (dto.include_facets as unknown) === true
      ) {
        includeFacets = true;
      } else if (
        (dto.include_facets as unknown) === 'false' ||
        (dto.include_facets as unknown) === false
      ) {
        includeFacets = false;
      } else {
        throw new SearchException(
          SearchErrorCode.SEARCH_INVALID_INPUT,
          ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
          ['include_facets must be a boolean'],
        );
      }
    }

    // Validate attribute
    const attributes: Record<string, string[]> = {};
    if (
      dto.attribute !== undefined &&
      dto.attribute !== null &&
      (dto.attribute as unknown) !== ''
    ) {
      if (typeof dto.attribute !== 'object' || Array.isArray(dto.attribute)) {
        throw new SearchException(
          SearchErrorCode.SEARCH_INVALID_INPUT,
          ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
          ['attribute must be an object'],
        );
      }

      const rawAttr = dto.attribute as Record<string, unknown>;

      if (
        Object.prototype.hasOwnProperty.call(rawAttr, '__proto__') ||
        Object.prototype.hasOwnProperty.call(rawAttr, 'constructor') ||
        Object.prototype.hasOwnProperty.call(rawAttr, 'prototype') ||
        (Object.getPrototypeOf(rawAttr) !== Object.prototype &&
          Object.getPrototypeOf(rawAttr) !== null)
      ) {
        throw new SearchException(
          SearchErrorCode.SEARCH_INVALID_INPUT,
          ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
          ['Dangerous attribute key detected'],
        );
      }

      const keys = Object.keys(rawAttr);

      if (keys.length > 10) {
        throw new SearchException(
          SearchErrorCode.SEARCH_INVALID_INPUT,
          ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
          ['attribute keys limit exceeded (max 10)'],
        );
      }

      for (const rawKey of keys) {
        if (DANGEROUS_KEYS.has(rawKey) || !ATTR_KEY_REGEX.test(rawKey)) {
          throw new SearchException(
            SearchErrorCode.SEARCH_INVALID_INPUT,
            ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
            [`Invalid attribute key '${rawKey}'`],
          );
        }

        const trimmedKey = rawKey.trim();
        const rawVal = rawAttr[rawKey];
        const valArr = Array.isArray(rawVal) ? rawVal : [rawVal];

        if (valArr.length > 20) {
          throw new SearchException(
            SearchErrorCode.SEARCH_INVALID_INPUT,
            ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
            [`Attribute '${rawKey}' has too many values (max 20)`],
          );
        }

        const cleanedValues: string[] = [];
        for (const v of valArr) {
          if (typeof v !== 'string' || v.length > 100) {
            throw new SearchException(
              SearchErrorCode.SEARCH_INVALID_INPUT,
              ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
              [`Invalid attribute value for key '${rawKey}'`],
            );
          }
          const trimmedV = v.trim();
          if (trimmedV.length > 0) {
            cleanedValues.push(trimmedV);
          }
        }

        if (cleanedValues.length > 0) {
          attributes[trimmedKey] = Array.from(new Set(cleanedValues));
        }
      }

      for (const prop of Object.getOwnPropertyNames(rawAttr)) {
        if (DANGEROUS_KEYS.has(prop)) {
          throw new SearchException(
            SearchErrorCode.SEARCH_INVALID_INPUT,
            ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
            [`Dangerous attribute key '${prop}'`],
          );
        }
      }
    }

    return {
      text,
      filters: {
        categoryId,
        shopId,
        brands,
        minPrice,
        maxPrice,
        attributes,
      },
      sort,
      page,
      size,
      includeFacets,
    };
  }
}
