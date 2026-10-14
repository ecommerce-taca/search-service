import { Transform } from 'class-transformer';
import {
  IsDefined,
  IsEnum,
  IsInt,
  IsNotEmpty,
  IsPositive,
  IsString,
  Length,
  Matches,
} from 'class-validator';
import { ReindexSource } from '../enums';

export class ReindexRequestDto {
  @IsDefined({ message: 'source is required' })
  @IsEnum(ReindexSource, {
    message: 'source must be either INDEX_COPY or KAFKA_REPLAY',
  })
  source!: ReindexSource;

  @IsDefined({ message: 'mapping_version is required' })
  @IsInt({ message: 'mapping_version must be an integer' })
  @IsPositive({ message: 'mapping_version must be a positive integer' })
  mapping_version!: number;

  @IsDefined({ message: 'reason is required' })
  @IsString({ message: 'reason must be a string' })
  @IsNotEmpty({ message: 'reason cannot be empty' })
  @Transform(({ value }: { value: unknown }) =>
    typeof value === 'string' ? value.trim() : value,
  )
  @Matches(/\S/, { message: 'reason cannot be empty or whitespace-only' })
  @Length(3, 200, { message: 'reason length constraint exceeded' })
  reason!: string;
}
