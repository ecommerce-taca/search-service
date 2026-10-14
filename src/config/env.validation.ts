import { plainToInstance, Type } from 'class-transformer';
import {
  IsInt,
  IsNotEmpty,
  IsOptional,
  IsString,
  validateSync,
} from 'class-validator';

export class EnvironmentVariables {
  @IsInt()
  @Type(() => Number)
  @IsOptional()
  PORT: number = 3000;

  @IsString()
  @IsNotEmpty()
  @IsOptional()
  SERVICE_NAME: string = 'search-service';

  @IsString()
  @IsNotEmpty()
  @IsOptional()
  APP_ENV: string = 'development';

  @IsString()
  @IsNotEmpty()
  @IsOptional()
  APP_VERSION: string = '1.0.0';

  @IsString()
  @IsNotEmpty({ message: 'ES_NODE is required and must not be empty' })
  ES_NODE!: string;

  @IsString()
  @IsOptional()
  ES_INDEX_PREFIX: string = '';

  @IsString()
  @IsNotEmpty({ message: 'KAFKA_BROKERS is required and must not be empty' })
  KAFKA_BROKERS!: string;

  @IsString()
  @IsNotEmpty()
  @IsOptional()
  KAFKA_CLIENT_ID: string = 'search-service';

  @IsString()
  @IsNotEmpty()
  @IsOptional()
  KAFKA_GROUP_ID: string = 'search-service-group';

  @IsString()
  @IsOptional()
  KAFKA_TOPIC_PREFIX: string = '';
}

export function validate(
  config: Record<string, unknown>,
): EnvironmentVariables {
  const validatedConfig = plainToInstance(EnvironmentVariables, config, {
    enableImplicitConversion: true,
  });

  const errors = validateSync(validatedConfig, {
    skipMissingProperties: false,
  });

  if (errors.length > 0) {
    const errorMessages = errors
      .map((err) => Object.values(err.constraints || {}).join(', '))
      .join('; ');
    throw new Error(`Config validation error (fail-fast): ${errorMessages}`);
  }

  return validatedConfig;
}
