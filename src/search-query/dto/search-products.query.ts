import {
  IsArray,
  IsBoolean,
  IsEnum,
  IsInt,
  IsOptional,
  IsString,
  Max,
  Min,
  Validate,
  ValidationArguments,
  ValidatorConstraint,
  ValidatorConstraintInterface,
} from 'class-validator';
import { Transform } from 'class-transformer';
import { SortOption } from '../../common/enums';

const DANGEROUS_KEYS = new Set(['__proto__', 'constructor', 'prototype']);
const ATTR_KEY_REGEX = /^[a-z0-9_]{1,64}$/i;

@ValidatorConstraint({ name: 'IsAttributeRecord', async: false })
export class IsAttributeRecordConstraint implements ValidatorConstraintInterface {
  validate(value: unknown): boolean {
    if (value === undefined || value === null) {
      return true;
    }
    if (typeof value !== 'object' || Array.isArray(value)) {
      return false;
    }

    const obj = value as Record<string, unknown>;

    if (
      Object.prototype.hasOwnProperty.call(obj, '__proto__') ||
      Object.prototype.hasOwnProperty.call(obj, 'constructor') ||
      Object.prototype.hasOwnProperty.call(obj, 'prototype') ||
      (Object.getPrototypeOf(obj) !== Object.prototype &&
        Object.getPrototypeOf(obj) !== null)
    ) {
      return false;
    }

    const keys = Object.keys(obj);

    if (keys.length > 10) {
      return false;
    }

    for (const key of keys) {
      if (DANGEROUS_KEYS.has(key)) {
        return false;
      }
      if (!ATTR_KEY_REGEX.test(key)) {
        return false;
      }

      const val = obj[key];
      if (typeof val === 'string') {
        if (val.length > 100) return false;
      } else if (Array.isArray(val)) {
        if (val.length > 20) return false;
        for (const item of val) {
          if (typeof item !== 'string' || item.length > 100) {
            return false;
          }
        }
      } else {
        return false;
      }
    }

    for (const prop of Object.getOwnPropertyNames(obj)) {
      if (DANGEROUS_KEYS.has(prop)) {
        return false;
      }
    }

    return true;
  }

  defaultMessage(args: ValidationArguments): string {
    return `Tham số '${args.property}' chứa thuộc tính không hợp lệ.`;
  }
}

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
  @IsOptional()
  @IsString()
  q?: string;

  @IsOptional()
  @IsString()
  category_id?: string;

  @IsOptional()
  @IsString()
  shop_id?: string;

  @IsOptional()
  @Transform(({ value }) => {
    if (value === undefined || value === null || value === '') {
      return undefined;
    }
    if (Array.isArray(value)) {
      return value;
    }
    return [value];
  })
  @IsArray()
  @IsString({ each: true })
  brand?: string[];

  @IsOptional()
  @Transform(({ value }) => transformPrice(value))
  @IsInt()
  @Min(0)
  @Max(1000000000000)
  min_price?: number;

  @IsOptional()
  @Transform(({ value }) => transformPrice(value))
  @IsInt()
  @Min(0)
  @Max(1000000000000)
  max_price?: number;

  @IsOptional()
  @IsEnum(SortOption)
  sort?: SortOption;

  @IsOptional()
  @Transform(({ value }) => transformInt(value, 1))
  @IsInt()
  @Min(1)
  page: number = 1;

  @IsOptional()
  @Transform(({ value }) => transformInt(value, 20))
  @IsInt()
  @Min(1)
  @Max(100)
  size: number = 20;

  @IsOptional()
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
    return 'INVALID_BOOLEAN';
  })
  @IsBoolean()
  include_facets: boolean = true;

  @IsOptional()
  @Validate(IsAttributeRecordConstraint)
  attribute?: Record<string, string | string[]>;
}
