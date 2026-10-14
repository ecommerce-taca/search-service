import { ReindexJobDocument } from './job.repository';

export interface ReindexSource {
  copy(
    job: ReindexJobDocument,
    targetIndex?: string,
    signal?: AbortSignal,
  ): Promise<void>;
}

export type IReindexSource = ReindexSource;
