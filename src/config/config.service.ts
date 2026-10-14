import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { EnvironmentVariables } from './env.validation';

@Injectable()
export class AppConfigService {
  constructor(
    private readonly configService: ConfigService<EnvironmentVariables, true>,
  ) {}

  get port(): number {
    return this.configService.get('PORT', { infer: true }) ?? 3000;
  }

  get serviceName(): string {
    return (
      this.configService.get('SERVICE_NAME', { infer: true }) ??
      'search-service'
    );
  }

  get appEnv(): string {
    return this.configService.get('APP_ENV', { infer: true }) ?? 'development';
  }

  get appVersion(): string {
    return this.configService.get('APP_VERSION', { infer: true }) ?? '1.0.0';
  }

  get esNode(): string {
    return this.configService.get('ES_NODE', { infer: true });
  }

  get esIndexPrefix(): string {
    return this.configService.get('ES_INDEX_PREFIX', { infer: true }) ?? '';
  }

  get kafkaBrokers(): string[] {
    const raw = this.configService.get('KAFKA_BROKERS', { infer: true });
    return raw
      ? raw
          .split(',')
          .map((b) => b.trim())
          .filter((b) => b.length > 0)
      : [];
  }

  get kafkaClientId(): string {
    return (
      this.configService.get('KAFKA_CLIENT_ID', { infer: true }) ??
      'search-service'
    );
  }

  get kafkaGroupId(): string {
    return (
      this.configService.get('KAFKA_GROUP_ID', { infer: true }) ??
      'search-service-group'
    );
  }

  get kafkaTopicPrefix(): string {
    return this.configService.get('KAFKA_TOPIC_PREFIX', { infer: true }) ?? '';
  }

  get kafkaConsumerEnabled(): boolean {
    return (
      this.configService.get('KAFKA_CONSUMER_ENABLED', { infer: true }) ?? true
    );
  }
}
