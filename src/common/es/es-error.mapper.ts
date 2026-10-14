import { errors } from '@elastic/elasticsearch';
import { SearchErrorCode } from '../errors/error-codes';
import { SearchException } from '../errors/search.exception';

function isElasticsearchError(err: unknown): boolean {
  if (err instanceof errors.ElasticsearchClientError) {
    return true;
  }
  if (err && typeof err === 'object') {
    const name = (err as { name?: string }).name;
    if (
      name &&
      (name === 'ElasticsearchClientError' ||
        name === 'TimeoutError' ||
        name === 'ConnectionError' ||
        name === 'NoLivingConnectionsError' ||
        name === 'ResponseError' ||
        name === 'ConfigurationError' ||
        name === 'SerializationError' ||
        name === 'DeserializationError' ||
        name === 'RequestAbortedError' ||
        name === 'ProductNotSupportedError' ||
        name.toLowerCase().includes('elastic'))
    ) {
      return true;
    }
  }
  return false;
}

export function toSearchException(err: unknown): SearchException {
  if (err instanceof SearchException) {
    return err;
  }

  if (
    err instanceof errors.TimeoutError ||
    (err as { name?: string })?.name === 'TimeoutError'
  ) {
    return new SearchException(SearchErrorCode.SEARCH_TIMEOUT);
  }

  if (
    err instanceof errors.ResponseError ||
    (err as { name?: string })?.name === 'ResponseError'
  ) {
    const resErr = err as errors.ResponseError;
    const statusCode = resErr.statusCode ?? resErr.meta?.statusCode;
    const body = resErr.meta?.body as { error?: { type?: string } } | undefined;
    const errorType = body?.error?.type;

    if (errorType === 'index_not_found_exception' || statusCode === 404) {
      return new SearchException(SearchErrorCode.SEARCH_INDEX_NOT_READY);
    }

    return new SearchException(SearchErrorCode.SEARCH_UNAVAILABLE);
  }

  if (isElasticsearchError(err)) {
    return new SearchException(SearchErrorCode.SEARCH_UNAVAILABLE);
  }

  throw err;
}
