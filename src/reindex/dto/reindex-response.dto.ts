import { ReindexState } from '../../common/enums';

export interface ReindexStartResponseData {
  job_id: string;
  state: ReindexState;
  target_index: string;
}

export interface AdminSearchStatusResponseData {
  alias: string;
  active_index: string | null;
  mapping_version: number;
  document_count: number;
  last_event_at: string | null;
  consumer_lag: Record<string, number>;
  dlq_count: number;
  reindex_state: string;
}

export interface ReindexJobDetailsResponseData {
  job_id: string;
  state: ReindexState;
  target_index: string;
  mapping_version: number;
  processed_count: number;
  failed_count: number;
  total_estimate: number | null;
  checkpoint: string | null;
  started_at: string;
  updated_at: string;
  error_code: string | null;
}
