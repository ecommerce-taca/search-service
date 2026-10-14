import { Global, Module } from '@nestjs/common';
import { Kafka } from 'kafkajs';
import { AppConfigModule } from '../config/config.module';
import { AppConfigService } from '../config/config.service';

export const KAFKA_CLIENT = 'KAFKA_CLIENT';

@Global()
@Module({
  imports: [AppConfigModule],
  providers: [
    {
      provide: KAFKA_CLIENT,
      inject: [AppConfigService],
      useFactory: (config: AppConfigService) => {
        return new Kafka({
          clientId: config.kafkaClientId,
          brokers: config.kafkaBrokers,
        });
      },
    },
    {
      provide: Kafka,
      useExisting: KAFKA_CLIENT,
    },
  ],
  exports: [KAFKA_CLIENT, Kafka],
})
export class KafkaModule {}
