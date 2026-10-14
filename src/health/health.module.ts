import { Module } from '@nestjs/common';
import { EventConsumerModule } from '../event-consumer/event-consumer.module';
import { HealthController } from './health.controller';
import { ReadinessController } from './readiness.controller';
import { ReadinessService } from './readiness.service';

@Module({
  imports: [EventConsumerModule],
  controllers: [HealthController, ReadinessController],
  providers: [ReadinessService],
  exports: [ReadinessService],
})
export class HealthModule {}
