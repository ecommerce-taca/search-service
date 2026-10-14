import { Module } from '@nestjs/common';
import { KafkaModule } from './kafka.module';

@Module({
  imports: [KafkaModule],
  exports: [KafkaModule],
})
export class EventConsumerModule {}
