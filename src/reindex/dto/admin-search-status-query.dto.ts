import { IsOptional, IsString, Matches } from 'class-validator';

export class AdminSearchStatusQueryDto {
  @IsOptional()
  @IsString({ message: 'index must be a string' })
  @Matches(/^[a-zA-Z0-9_.-]+$/, {
    message: 'index contains invalid characters',
  })
  index?: string;

  @IsOptional()
  @IsString({ message: 'consumer_group must be a string' })
  @Matches(/^[a-zA-Z0-9_.-]+$/, {
    message: 'consumer_group contains invalid characters',
  })
  consumer_group?: string;
}
