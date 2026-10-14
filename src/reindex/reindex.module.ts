import { Module } from '@nestjs/common';
import { AdminSearchController } from './admin-search.controller';
import { ReindexJobRepository } from './job.repository';
import { ReindexRunner } from './reindex.runner';
import { ReindexService } from './reindex.service';
import { ValidationGate } from './validation-gate';

@Module({
  controllers: [AdminSearchController],
  providers: [
    ReindexJobRepository,
    ValidationGate,
    ReindexRunner,
    ReindexService,
  ],
  exports: [
    ReindexJobRepository,
    ValidationGate,
    ReindexRunner,
    ReindexService,
  ],
})
export class ReindexModule {}
