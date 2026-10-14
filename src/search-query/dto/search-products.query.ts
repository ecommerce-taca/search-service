import { Transform } from 'class-transformer';
import { Allow } from 'class-validator';
import { SortOption } from '../../common/enums';

function transformPrice(val: unknown): number | typeof NaN | undefined {
  if (val === undefined || val === null || val === '') {
    return undefined;
  }
  if (typeof val === 'number') {
    return Number.isInteger(val) ? val : NaN;
  }
  if (typeof val === 'string') {
    const trimmed = val.trim();
    if (!/^\d+$/.test(trimmed)) {
      return NaN;
    }
    return Number(trimmed);
  }
  return NaN;
}

function transformInt(
  val: unknown,
  fallback: number,
): number | typeof NaN | undefined {
  if (val === undefined || val === null || val === '') {
    return fallback;
  }
  if (typeof val === 'number') {
    return Number.isInteger(val) ? val : NaN;
  }
  if (typeof val === 'string') {
    const trimmed = val.trim();
    if (!/^\d+$/.test(trimmed)) {
      return NaN;
    }
    return Number(trimmed);
  }
  return NaN;
}

export class SearchProductsQueryDto {
  @Allow()
  q?: string;

  @Allow()
  category_id?: string;

  @Allow()
  shop_id?: string;

  @Allow()
  @Transform(({ value }) => {
    if (value === undefined || value === null || value === '') {
      return undefined;
    }
    if (Array.isArray(value)) {
      return value;
    }
    return [value];
  })
  brand?: string[];

  @Allow()
  @Transform(({ value }) => transformPrice(value))
  min_price?: number;

  @Allow()
  @Transform(({ value }) => transformPrice(value))
  max_price?: number;

  @Allow()
  sort?: SortOption;

  @Allow()
  @Transform(({ value }) => transformInt(value, 1))
  page: number = 1;

  @Allow()
  @Transform(({ value }) => transformInt(value, 20))
  size: number = 20;

  @Allow()
  @Transform(({ value }) => {
    if (value === undefined || value === null || value === '') {
      return true;
    }
    if (value === true || value === 'true') {
      return true;
    }
    if (value === false || value === 'false') {
      return false;
    }
    return value;
  })
  include_facets: boolean = true;

  @Allow()
  attribute?: Record<string, string | string[]>;
}
