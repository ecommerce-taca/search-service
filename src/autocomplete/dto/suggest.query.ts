import { Transform, Type } from 'class-transformer';
import {
  IsDefined,
  IsInt,
  IsNotEmpty,
  IsOptional,
  IsString,
  Max,
  MaxLength,
  Min,
} from 'class-validator';

export class SuggestQueryDto {
  @IsDefined({ message: 'prefix is required' })
  @IsString({ message: 'prefix must be a string' })
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @IsNotEmpty({ message: 'prefix cannot be empty' })
  @MaxLength(100, { message: 'prefix exceeds max length of 100 characters' })
  prefix!: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt({ message: 'limit must be a positive integer between 1 and 10' })
  @Min(1, { message: 'limit must be a positive integer between 1 and 10' })
  @Max(10, { message: 'limit must not exceed 10' })
  limit?: number = 10;

  @IsOptional()
  @IsString()
  @IsNotEmpty()
  category_id?: string;
}
