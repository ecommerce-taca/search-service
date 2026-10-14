import { Module } from '@nestjs/common';
import { KafkaEventConsumer } from './consumer/kafka-event.consumer';
import {
  CatalogHandler,
  CategoryHandler,
  ProductHandler,
  RatingHandler,
  SkuHandler,
} from './handlers';
import { CONSUMER_STATUS_PROVIDER } from './interfaces';
import { KafkaModule } from './kafka.module';
import { ConsumerStatusService } from './status/consumer-status.service';

@Module({
  imports: [KafkaModule],
  providers: [
    ProductHandler,
    SkuHandler,
    CategoryHandler,
    CatalogHandler,
    RatingHandler,
    ConsumerStatusService,
    {
      provide: CONSUMER_STATUS_PROVIDER,
      useClass: ConsumerStatusService,
    },
    KafkaEventConsumer,
  ],
  exports: [
    KafkaModule,
    ProductHandler,
    SkuHandler,
    CategoryHandler,
    CatalogHandler,
    RatingHandler,
    ConsumerStatusService,
    CONSUMER_STATUS_PROVIDER,
    KafkaEventConsumer,
  ],
})
export class EventConsumerModule {}
