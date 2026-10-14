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
import {
  MAX_PRICE_VALUE,
  MAX_QUERY_ATTRIBUTE_KEYS,
  MAX_QUERY_ATTRIBUTE_VALUES_PER_KEY,
  MAX_QUERY_ATTRIBUTE_VALUE_LENGTH,
  MAX_RESULT_WINDOW,
} from './constants';
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
    if (!dto || typeof dto !== 'object' || Array.isArray(dto)) {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
        ['Invalid query object'],
      );
    }

    this.validateUnknownFields(dto as unknown as Record<string, unknown>);

    const text = this.parseText(dto.q);
    const categoryId = this.parseCategory(dto.category_id);
    const shopId = this.parseShop(dto.shop_id);
    const brands = this.parseBrands(dto.brand);
    const { minPrice, maxPrice } = this.parsePrice(
      dto.min_price,
      dto.max_price,
    );
    const sort = this.parseSort(dto.sort, text);
    const { page, size } = this.parsePaging(dto.page, dto.size);
    const includeFacets = this.parseFacetsFlag(dto.include_facets);
    const attributes = this.parseAttributes(dto.attribute);

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

  private validateUnknownFields(dto: Record<string, unknown>): void {
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
  }

  private parseText(q?: unknown): string | undefined {
    if (q === undefined || q === null) {
      return undefined;
    }
    if (typeof q !== 'string') {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
        ['q must be a string'],
      );
    }
    const codePointsLength = Array.from(q).length;
    if (codePointsLength > QUERY_MAX_LENGTH) {
      throw new SearchException(
        SearchErrorCode.SEARCH_QUERY_TOO_LONG,
        ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_QUERY_TOO_LONG],
      );
    }
    const trimmed = q.trim();
    return trimmed.length > 0 ? trimmed : undefined;
  }

  private parseCategory(categoryId?: unknown): string | undefined {
    if (categoryId === undefined || categoryId === null) {
      return undefined;
    }
    if (typeof categoryId !== 'string') {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
        ['category_id must be a string'],
      );
    }
    const trimmed = categoryId.trim();
    return trimmed.length > 0 ? trimmed : undefined;
  }

  private parseShop(shopId?: unknown): string | undefined {
    if (shopId === undefined || shopId === null) {
      return undefined;
    }
    if (typeof shopId !== 'string') {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
        ['shop_id must be a string'],
      );
    }
    const trimmed = shopId.trim();
    return trimmed.length > 0 ? trimmed : undefined;
  }

  private parseBrands(brand?: unknown): string[] {
    if (brand === undefined || brand === null || (brand as unknown) === '') {
      return [];
    }
    const rawBrands = Array.isArray(brand) ? brand : [brand];
    const brands: string[] = [];
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
    return brands;
  }

  private parsePrice(
    minPriceRaw?: unknown,
    maxPriceRaw?: unknown,
  ): { minPrice?: number; maxPrice?: number } {
    let minPrice: number | undefined;
    let maxPrice: number | undefined;

    if (
      minPriceRaw !== undefined &&
      minPriceRaw !== null &&
      (minPriceRaw as unknown) !== ''
    ) {
      minPrice = this.validateSinglePrice('min_price', minPriceRaw);
    }

    if (
      maxPriceRaw !== undefined &&
      maxPriceRaw !== null &&
      (maxPriceRaw as unknown) !== ''
    ) {
      maxPrice = this.validateSinglePrice('max_price', maxPriceRaw);
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

    return { minPrice, maxPrice };
  }

  private validateSinglePrice(field: string, val: unknown): number {
    let num: number;
    if (typeof val === 'number') {
      if (!Number.isInteger(val) || isNaN(val)) {
        throw new SearchException(
          SearchErrorCode.SEARCH_INVALID_INPUT,
          ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
          [`${field} must be an integer between 0 and 10^12`],
        );
      }
      num = val;
    } else if (typeof val === 'string') {
      const trimmed = val.trim();
      if (!/^\d+$/.test(trimmed)) {
        throw new SearchException(
          SearchErrorCode.SEARCH_INVALID_INPUT,
          ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
          [`${field} must be an integer between 0 and 10^12`],
        );
      }
      num = Number(trimmed);
    } else {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
        [`${field} must be an integer between 0 and 10^12`],
      );
    }

    if (num < 0 || num > MAX_PRICE_VALUE) {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
        [`${field} must be an integer between 0 and 10^12`],
      );
    }

    return num;
  }

  private parseSort(sortRaw?: unknown, text?: string): SortOption {
    if (
      sortRaw !== undefined &&
      sortRaw !== null &&
      (sortRaw as unknown) !== ''
    ) {
      if (!Object.values(SortOption).includes(sortRaw as SortOption)) {
        throw new SearchException(
          SearchErrorCode.SEARCH_INVALID_INPUT,
          ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
          [`Unsupported sort option '${sortRaw}'`],
        );
      }
      return sortRaw as SortOption;
    }
    return text ? SortOption.RELEVANCE : SortOption.NEWEST;
  }

  private parsePaging(
    pageRaw?: unknown,
    sizeRaw?: unknown,
  ): { page: number; size: number } {
    let page = 1;
    if (
      pageRaw !== undefined &&
      pageRaw !== null &&
      (pageRaw as unknown) !== ''
    ) {
      page = this.validateIntegerParam('page', pageRaw, 1, Infinity);
    }

    let size = PAGE_SIZE_DEFAULT;
    if (
      sizeRaw !== undefined &&
      sizeRaw !== null &&
      (sizeRaw as unknown) !== ''
    ) {
      size = this.validateIntegerParam('size', sizeRaw, 1, PAGE_SIZE_MAX);
    }

    if (page * size > MAX_RESULT_WINDOW) {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
        [`page * size cannot exceed ${MAX_RESULT_WINDOW}`],
      );
    }

    return { page, size };
  }

  private validateIntegerParam(
    field: 'page' | 'size',
    val: unknown,
    min: number,
    max: number,
  ): number {
    const errorMsg =
      field === 'page'
        ? 'page must be an integer >= 1'
        : 'size must be an integer between 1 and 100';

    let num: number;
    if (typeof val === 'number') {
      if (!Number.isInteger(val) || isNaN(val)) {
        throw new SearchException(
          SearchErrorCode.SEARCH_INVALID_INPUT,
          ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
          [errorMsg],
        );
      }
      num = val;
    } else if (typeof val === 'string') {
      const trimmed = val.trim();
      if (!/^\d+$/.test(trimmed)) {
        throw new SearchException(
          SearchErrorCode.SEARCH_INVALID_INPUT,
          ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
          [errorMsg],
        );
      }
      num = Number(trimmed);
    } else {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
        [errorMsg],
      );
    }

    if (num < min || num > max) {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
        [errorMsg],
      );
    }

    return num;
  }

  private parseFacetsFlag(includeFacetsRaw?: unknown): boolean {
    if (
      includeFacetsRaw === undefined ||
      includeFacetsRaw === null ||
      (includeFacetsRaw as unknown) === ''
    ) {
      return true;
    }
    if (typeof includeFacetsRaw === 'boolean') {
      return includeFacetsRaw;
    }
    if (
      (includeFacetsRaw as unknown) === 'true' ||
      (includeFacetsRaw as unknown) === true
    ) {
      return true;
    }
    if (
      (includeFacetsRaw as unknown) === 'false' ||
      (includeFacetsRaw as unknown) === false
    ) {
      return false;
    }
    throw new SearchException(
      SearchErrorCode.SEARCH_INVALID_INPUT,
      ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
      ['include_facets must be a boolean'],
    );
  }

  private parseAttributes(attributeRaw?: unknown): Record<string, string[]> {
    if (
      attributeRaw === undefined ||
      attributeRaw === null ||
      (attributeRaw as unknown) === ''
    ) {
      return {};
    }
    if (typeof attributeRaw !== 'object' || Array.isArray(attributeRaw)) {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
        ['attribute must be an object'],
      );
    }

    const rawAttr = attributeRaw as Record<string, unknown>;
    this.checkAttributePrototypes(rawAttr);

    const keys = Object.keys(rawAttr);
    if (keys.length > MAX_QUERY_ATTRIBUTE_KEYS) {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
        [`attribute keys limit exceeded (max ${MAX_QUERY_ATTRIBUTE_KEYS})`],
      );
    }

    const attributes: Record<string, string[]> = {};
    for (const rawKey of keys) {
      this.validateAttributeKey(rawKey);
      const cleaned = this.parseAttributeValues(rawKey, rawAttr[rawKey]);
      if (cleaned.length > 0) {
        attributes[rawKey.trim()] = Array.from(new Set(cleaned));
      }
    }
    return attributes;
  }

  private checkAttributePrototypes(rawAttr: Record<string, unknown>): void {
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

  private validateAttributeKey(key: string): void {
    if (DANGEROUS_KEYS.has(key) || !ATTR_KEY_REGEX.test(key)) {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
        [`Invalid attribute key '${key}'`],
      );
    }
  }

  private parseAttributeValues(key: string, rawVal: unknown): string[] {
    const valArr = Array.isArray(rawVal) ? rawVal : [rawVal];
    if (valArr.length > MAX_QUERY_ATTRIBUTE_VALUES_PER_KEY) {
      throw new SearchException(
        SearchErrorCode.SEARCH_INVALID_INPUT,
        ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
        [
          `Attribute '${key}' has too many values (max ${MAX_QUERY_ATTRIBUTE_VALUES_PER_KEY})`,
        ],
      );
    }
    const cleanedValues: string[] = [];
    for (const v of valArr) {
      if (
        typeof v !== 'string' ||
        v.length > MAX_QUERY_ATTRIBUTE_VALUE_LENGTH
      ) {
        throw new SearchException(
          SearchErrorCode.SEARCH_INVALID_INPUT,
          ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_INVALID_INPUT],
          [`Invalid attribute value for key '${key}'`],
        );
      }
      const trimmed = v.trim();
      if (trimmed.length > 0) {
        cleanedValues.push(trimmed);
      }
    }
    return cleanedValues;
  }
}
