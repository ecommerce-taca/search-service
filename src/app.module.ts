import { Module } from '@nestjs/common';
import { AppConfigModule } from './config/config.module';
import { ObservabilityModule } from './observability/observability.module';
import { HealthModule } from './health/health.module';
import { SearchQueryModule } from './search-query/search-query.module';
import { AutocompleteModule } from './autocomplete/autocomplete.module';
import { ProductIndexModule } from './product-index/product-index.module';
import { EventConsumerModule } from './event-consumer/event-consumer.module';
import { ReindexModule } from './reindex/reindex.module';
import { SecurityModule } from './security/security.module';

@Module({
  imports: [
    AppConfigModule,
    ObservabilityModule,
    HealthModule,
    SearchQueryModule,
    AutocompleteModule,
    ProductIndexModule,
    EventConsumerModule,
    ReindexModule,
    SecurityModule,
  ],
})
export class AppModule {}
