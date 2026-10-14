import { Module } from '@nestjs/common';
import { EventConsumerModule } from '../event-consumer/event-consumer.module';
import { AdminSearchController } from './admin-search.controller';
import { IndexCopySource } from './index-copy.source';
import { ReindexJobRepository } from './job.repository';
import { KafkaReplaySource } from './kafka-replay.source';
import { ReindexRunner } from './reindex.runner';
import { ReindexService } from './reindex.service';
import { ValidationGate } from './validation-gate';

@Module({
  imports: [EventConsumerModule],
  controllers: [AdminSearchController],
  providers: [
    ReindexJobRepository,
    ValidationGate,
    IndexCopySource,
    KafkaReplaySource,
    ReindexRunner,
    ReindexService,
  ],
  exports: [
    ReindexJobRepository,
    ValidationGate,
    IndexCopySource,
    KafkaReplaySource,
    ReindexRunner,
    ReindexService,
  ],
})
export class ReindexModule {}
