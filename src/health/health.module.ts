import { Module } from '@nestjs/common';
import { HealthController } from './health.controller';
import { ReadinessController } from './readiness.controller';
import { ReadinessService } from './readiness.service';

@Module({
  controllers: [HealthController, ReadinessController],
  providers: [ReadinessService],
  exports: [ReadinessService],
})
export class HealthModule {}
