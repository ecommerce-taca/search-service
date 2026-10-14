import { Client } from '@elastic/elasticsearch';
import { Injectable, Logger } from '@nestjs/common';
import { getProductsAlias } from '../common/constants';
import { ReindexState } from '../common/enums';
import { AppConfigService } from '../config/config.service';
import { ReindexErrorCode, ReindexSource as ReindexSourceEnum } from './enums';
import { IndexCopySource } from './index-copy.source';
import { ReindexJobDocument, ReindexJobRepository } from './job.repository';
import { KafkaReplaySource } from './kafka-replay.source';
import { ReindexSource } from './reindex-source.interface';
import { assertTransition } from './state-machine';
import { ValidationGate } from './validation-gate';

@Injectable()
export class ReindexRunner {
  private readonly logger = new Logger(ReindexRunner.name);
  private readonly abortControllers = new Map<string, AbortController>();
  private readonly abortedJobIds = new Set<string>();

  constructor(
    private readonly client: Client,
    private readonly configService: AppConfigService,
    private readonly jobRepo: ReindexJobRepository,
    private readonly validationGate: ValidationGate,
    private readonly indexCopySource: IndexCopySource,
    private readonly kafkaReplaySource: KafkaReplaySource,
  ) {}

  abort(jobId: string): void {
    this.abortedJobIds.add(jobId);
    const controller = this.abortControllers.get(jobId);
    if (controller) {
      controller.abort();
    }
  }

  private isAbortedOrCancelled(
    job: ReindexJobDocument,
    signal?: AbortSignal,
  ): boolean {
    const internalSignal = this.abortControllers.get(job.job_id)?.signal;
    return Boolean(
      this.abortedJobIds.has(job.job_id) ||
      signal?.aborted ||
      internalSignal?.aborted ||
      (job.state as ReindexState) === ReindexState.CANCELLED,
    );
  }

  private getSourceStrategy(source?: string): ReindexSource {
    if (source === ReindexSourceEnum.KAFKA_REPLAY) {
      return this.kafkaReplaySource;
    }
    return this.indexCopySource;
  }

  async run(job: ReindexJobDocument, signal?: AbortSignal): Promise<void> {
    const controller = new AbortController();
    this.abortControllers.set(job.job_id, controller);
    if (this.abortedJobIds.has(job.job_id)) {
      controller.abort();
    }
    if (signal) {
      if (signal.aborted) {
        controller.abort();
      } else {
        signal.addEventListener('abort', () => controller.abort());
      }
    }

    try {
      const alias = getProductsAlias(this.configService.esIndexPrefix);
      let activeIndex: string | null = null;
      try {
        const aliasMap = await this.client.indices.getAlias({ name: alias });
        const indices = Object.keys(aliasMap).sort();
        if (indices.length > 0) {
          activeIndex = indices[0];
        }
      } catch {
        // Alias does not exist
      }

      await this.executeJob(job, activeIndex, controller.signal);
    } finally {
      this.abortControllers.delete(job.job_id);
      this.abortedJobIds.delete(job.job_id);
    }
  }

  private async executeJob(
    job: ReindexJobDocument,
    activeIndex: string | null,
    signal: AbortSignal,
  ): Promise<void> {
    try {
      if (this.isAbortedOrCancelled(job, signal)) return;

      // 1. Transition to RUNNING
      assertTransition(job.state, ReindexState.RUNNING);
      job.state = ReindexState.RUNNING;
      job.updated_at = new Date().toISOString();
      if (this.isAbortedOrCancelled(job, signal)) return;
      await this.jobRepo.update(job);

      // 2. Execute copy step
      await this.copyStep(job, signal);
      if ((job.state as ReindexState) === ReindexState.FAILED) return;
      if (this.isAbortedOrCancelled(job, signal)) return;

      // 3. Transition to VALIDATING
      assertTransition(job.state, ReindexState.VALIDATING);
      job.state = ReindexState.VALIDATING;
      job.updated_at = new Date().toISOString();
      if (this.isAbortedOrCancelled(job, signal)) return;
      await this.jobRepo.update(job);

      // 4. Run validation gates
      await this.gateStep(job, activeIndex);
      if (this.isAbortedOrCancelled(job, signal)) return;

      // 5. Swap alias
      await this.swapStep(job, activeIndex);
    } catch (err: unknown) {
      if (this.isAbortedOrCancelled(job, signal)) return;

      const errorClass =
        (err as object)?.constructor?.name || typeof err || 'UnknownError';
      this.logger.error(`Reindex runner caught error [${errorClass}]`);

      if (!job.error_code) {
        job.error_code = ReindexErrorCode.REINDEX_WORKER_ERROR;
      }
      job.state = ReindexState.FAILED;
      job.finished_at = new Date().toISOString();
      job.updated_at = new Date().toISOString();

      try {
        await this.jobRepo.update(job);
      } catch (repoErr) {
        const repoErrClass =
          (repoErr as object)?.constructor?.name ||
          typeof repoErr ||
          'UnknownError';
        this.logger.warn(
          `Failed to persist failed job state: [${repoErrClass}]`,
        );
      }
    } finally {
      if (!this.isAbortedOrCancelled(job, signal)) {
        await this.jobRepo.releaseLock(job.job_id);
      }
    }
  }

  private async copyStep(
    job: ReindexJobDocument,
    signal?: AbortSignal,
  ): Promise<void> {
    const strategy = this.getSourceStrategy(job.source);
    await strategy.copy(job, job.target_index, signal);
  }

  private async gateStep(
    job: ReindexJobDocument,
    activeIndex: string | null,
  ): Promise<void> {
    const gateResult = await this.validationGate.run(
      job.target_index,
      activeIndex,
    );
    if (!gateResult.pass) {
      job.error_code =
        gateResult.reason ||
        gateResult.errorCode ||
        ReindexErrorCode.REINDEX_WORKER_ERROR;
      throw new Error(`Validation gate failed: ${job.error_code}`);
    }
  }

  private async swapStep(
    job: ReindexJobDocument,
    activeIndex: string | null,
  ): Promise<void> {
    const alias = getProductsAlias(this.configService.esIndexPrefix);
    const actions: Array<{
      remove?: { index: string; alias: string };
      add?: { index: string; alias: string };
    }> = [];

    if (activeIndex) {
      actions.push({ remove: { index: activeIndex, alias } });
    }
    actions.push({ add: { index: job.target_index, alias } });

    await this.client.indices.updateAliases({ body: { actions } });
    this.logger.log(
      `Swapped alias ${alias} to ${job.target_index}. Old index ${activeIndex ?? 'none'} retained.`,
    );

    assertTransition(job.state, ReindexState.SWAPPED);
    job.state = ReindexState.SWAPPED;
    job.finished_at = new Date().toISOString();
    job.updated_at = new Date().toISOString();
    job.error_code = null;
    await this.jobRepo.update(job);
  }
}
