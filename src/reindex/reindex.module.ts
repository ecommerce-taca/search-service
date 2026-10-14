import { Module } from '@nestjs/common';
import { CONSUMER_STATUS_PROVIDER } from '../event-consumer/interfaces';
import { AdminSearchController } from './admin-search.controller';
import { IndexCopySource } from './index-copy.source';
import { ReindexJobRepository } from './job.repository';
import { KafkaReplaySource } from './kafka-replay.source';
import { ReindexRunner } from './reindex.runner';
import { ReindexService } from './reindex.service';
import { ValidationGate } from './validation-gate';

@Module({
  controllers: [AdminSearchController],
  providers: [
    {
      provide: CONSUMER_STATUS_PROVIDER,
      useValue: null,
    },
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
