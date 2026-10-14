import { Client, estypes } from '@elastic/elasticsearch';
import { Injectable, Logger } from '@nestjs/common';
import { getProductsAlias } from '../common/constants';
import { AppConfigService } from '../config/config.service';
import { ReindexErrorCode } from './enums';
import { ReindexJobDocument, ReindexJobRepository } from './job.repository';
import { ReindexSource } from './reindex-source.interface';

@Injectable()
export class IndexCopySource implements ReindexSource {
  private readonly logger = new Logger(IndexCopySource.name);

  constructor(
    private readonly client: Client,
    private readonly configService: AppConfigService,
    private readonly jobRepo: ReindexJobRepository,
  ) {}

  async copy(
    job: ReindexJobDocument,
    targetIndex?: string,
    signal?: AbortSignal,
  ): Promise<void> {
    const destIndex = targetIndex || job.target_index;
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

    if (!activeIndex) {
      job.processed_count = 0;
      job.failed_count = 0;
      job.updated_at = new Date().toISOString();
      await this.jobRepo.update(job);
      return;
    }

    const reindexTask = await this.client.reindex({
      wait_for_completion: false,
      conflicts: 'proceed',
      source: { index: activeIndex },
      dest: { index: destIndex },
    });

    const rawTaskId = (reindexTask as { task?: string | number }).task;
    const taskId = rawTaskId != null ? String(rawTaskId) : null;
    job.checkpoint = taskId;
    job.updated_at = new Date().toISOString();
    await this.jobRepo.updateProgress(job.job_id, {
      checkpoint: taskId,
      processed_count: job.processed_count,
      failed_count: job.failed_count,
    });

    if (!taskId) return;

    while (!signal?.aborted) {
      await new Promise((resolve) => setTimeout(resolve, 1000));
      if (signal?.aborted) break;

      const taskInfo: estypes.TasksGetResponse = await this.client.tasks.get({
        task_id: taskId,
      });

      if (signal?.aborted) break;

      const status = taskInfo.task?.status as
        | {
            total?: number;
            created?: number;
            updated?: number;
            version_conflicts?: number;
          }
        | undefined;
      const response = taskInfo.response as
        | {
            created?: number;
            updated?: number;
            version_conflicts?: number;
            failures?: unknown[];
          }
        | undefined;

      job.processed_count =
        (response?.created ?? status?.created ?? 0) +
        (response?.updated ?? status?.updated ?? 0);
      job.failed_count =
        response?.version_conflicts ?? status?.version_conflicts ?? 0;
      job.updated_at = new Date().toISOString();

      if (signal?.aborted) break;

      await this.jobRepo.updateProgress(job.job_id, {
        checkpoint: taskId,
        processed_count: job.processed_count,
        failed_count: job.failed_count,
      });

      if (taskInfo.completed) {
        if (
          taskInfo.error ||
          (response?.failures && response.failures.length > 0)
        ) {
          job.error_code = ReindexErrorCode.REINDEX_WORKER_ERROR;
          throw new Error(`Reindex task failed: ${taskId}`);
        }
        break;
      }
    }
  }
}
