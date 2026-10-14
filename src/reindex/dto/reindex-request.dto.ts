import { Transform } from 'class-transformer';
import {
  Equals,
  IsDefined,
  IsEnum,
  IsInt,
  IsNotEmpty,
  IsOptional,
  IsPositive,
  IsString,
  Length,
  Matches,
} from 'class-validator';
import { MAPPING_VERSION } from '../../common/constants';
import { ReindexSource } from '../enums';

export class ReindexRequestDto {
  @IsOptional()
  @IsEnum(ReindexSource, {
    message: 'source must be either INDEX_COPY or KAFKA_REPLAY',
  })
  @Transform(({ value }: { value: unknown }) =>
    value !== undefined && value !== null ? value : ReindexSource.INDEX_COPY,
  )
  source: ReindexSource = ReindexSource.INDEX_COPY;

  @IsDefined({ message: 'mapping_version is required' })
  @IsInt({ message: 'mapping_version must be an integer' })
  @IsPositive({ message: 'mapping_version must be a positive integer' })
  @Equals(MAPPING_VERSION, {
    message: `mapping_version must be ${MAPPING_VERSION}`,
  })
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

export { ReindexSource };
export {
  AdminSearchStatusQueryDto as StatusQueryDto,
  AdminSearchStatusQueryDto,
} from './admin-search-status-query.dto';
