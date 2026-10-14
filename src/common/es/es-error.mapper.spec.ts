import { errors } from '@elastic/elasticsearch';
import { HttpStatus } from '@nestjs/common';
import { SearchErrorCode } from '../errors/error-codes';
import { SearchException } from '../errors/search.exception';
import { toSearchException } from './es-error.mapper';

describe('toSearchException', () => {
  it('should pass through existing SearchException unchanged', () => {
    const original = new SearchException(
      SearchErrorCode.SEARCH_INVALID_INPUT,
      'Invalid params',
      { field: 'q' },
    );

    const result = toSearchException(original);

    expect(result).toBe(original);
    expect(result.code).toBe(SearchErrorCode.SEARCH_INVALID_INPUT);
    expect(result.getStatus()).toBe(HttpStatus.BAD_REQUEST);
  });

  it('should map errors.TimeoutError to SEARCH_TIMEOUT with 504 status', () => {
    const timeoutErr = new errors.TimeoutError(
      'ES request timed out',
      {} as unknown as ConstructorParameters<typeof errors.TimeoutError>[1],
    );

    const result = toSearchException(timeoutErr);

    expect(result).toBeInstanceOf(SearchException);
    expect(result.code).toBe(SearchErrorCode.SEARCH_TIMEOUT);
    expect(result.getStatus()).toBe(HttpStatus.GATEWAY_TIMEOUT);
  });

  it('should map errors.ResponseError with index_not_found_exception to SEARCH_INDEX_NOT_READY with 503 status', () => {
    const notFoundErr = new errors.ResponseError({
      statusCode: 404,
      body: {
        error: {
          type: 'index_not_found_exception',
          reason: 'no such index [products-v1]',
        },
      },
      warnings: [],
      headers: {},
    } as unknown as ConstructorParameters<typeof errors.ResponseError>[0]);

    const result = toSearchException(notFoundErr);

    expect(result).toBeInstanceOf(SearchException);
    expect(result.code).toBe(SearchErrorCode.SEARCH_INDEX_NOT_READY);
    expect(result.getStatus()).toBe(HttpStatus.SERVICE_UNAVAILABLE);
  });

  it('should map errors.ResponseError with statusCode 404 to SEARCH_INDEX_NOT_READY even if body error.type differs', () => {
    const err404 = new errors.ResponseError({
      statusCode: 404,
      body: {
        error: {
          type: 'resource_not_found_exception',
        },
      },
      warnings: [],
      headers: {},
    } as unknown as ConstructorParameters<typeof errors.ResponseError>[0]);

    const result = toSearchException(err404);

    expect(result).toBeInstanceOf(SearchException);
    expect(result.code).toBe(SearchErrorCode.SEARCH_INDEX_NOT_READY);
    expect(result.getStatus()).toBe(HttpStatus.SERVICE_UNAVAILABLE);
  });

  it('should map errors.ResponseError with index_not_found_exception to SEARCH_INDEX_NOT_READY when status is not 404', () => {
    const err400IndexNotFound = new errors.ResponseError({
      statusCode: 400,
      body: {
        error: {
          type: 'index_not_found_exception',
        },
      },
      warnings: [],
      headers: {},
    } as unknown as ConstructorParameters<typeof errors.ResponseError>[0]);

    const result = toSearchException(err400IndexNotFound);

    expect(result).toBeInstanceOf(SearchException);
    expect(result.code).toBe(SearchErrorCode.SEARCH_INDEX_NOT_READY);
  });

  it('should map errors.ConnectionError to SEARCH_UNAVAILABLE with 503 status', () => {
    const connErr = new errors.ConnectionError(
      'Connection refused',
      {} as unknown as ConstructorParameters<typeof errors.ConnectionError>[1],
    );

    const result = toSearchException(connErr);

    expect(result).toBeInstanceOf(SearchException);
    expect(result.code).toBe(SearchErrorCode.SEARCH_UNAVAILABLE);
    expect(result.getStatus()).toBe(HttpStatus.SERVICE_UNAVAILABLE);
  });

  it('should map errors.NoLivingConnectionsError to SEARCH_UNAVAILABLE with 503 status', () => {
    const noLivingErr = new errors.NoLivingConnectionsError(
      'No living connections',
      {} as unknown as ConstructorParameters<
        typeof errors.NoLivingConnectionsError
      >[1],
    );

    const result = toSearchException(noLivingErr);

    expect(result).toBeInstanceOf(SearchException);
    expect(result.code).toBe(SearchErrorCode.SEARCH_UNAVAILABLE);
    expect(result.getStatus()).toBe(HttpStatus.SERVICE_UNAVAILABLE);
  });

  it('should map errors.ResponseError >= 500 to SEARCH_UNAVAILABLE', () => {
    const err500 = new errors.ResponseError({
      statusCode: 500,
      body: { error: { type: 'search_phase_execution_exception' } },
      warnings: [],
      headers: {},
    } as unknown as ConstructorParameters<typeof errors.ResponseError>[0]);

    const err503 = new errors.ResponseError({
      statusCode: 503,
      body: { error: { type: 'node_not_connected_exception' } },
      warnings: [],
      headers: {},
    } as unknown as ConstructorParameters<typeof errors.ResponseError>[0]);

    expect(toSearchException(err500).code).toBe(
      SearchErrorCode.SEARCH_UNAVAILABLE,
    );
    expect(toSearchException(err503).code).toBe(
      SearchErrorCode.SEARCH_UNAVAILABLE,
    );
  });

  it('should map other ElasticsearchClientError subclasses to SEARCH_UNAVAILABLE', () => {
    const configErr = new errors.ConfigurationError('Invalid ES configuration');
    const serErr = new errors.SerializationError(
      'Serialization failed',
      {} as unknown as ConstructorParameters<
        typeof errors.SerializationError
      >[1],
    );
    const deserErr = new errors.DeserializationError(
      'Deserialization failed',
      {} as unknown as ConstructorParameters<
        typeof errors.DeserializationError
      >[1],
    );
    const abortErr = new errors.RequestAbortedError(
      'Request aborted',
      {} as unknown as ConstructorParameters<
        typeof errors.RequestAbortedError
      >[1],
    );
    const productErr = new errors.ProductNotSupportedError(
      'Unsupported cluster',
      {} as unknown as ConstructorParameters<
        typeof errors.ProductNotSupportedError
      >[1],
    );
    const err400Other = new errors.ResponseError({
      statusCode: 400,
      body: { error: { type: 'illegal_argument_exception' } },
      warnings: [],
      headers: {},
    } as unknown as ConstructorParameters<typeof errors.ResponseError>[0]);

    expect(toSearchException(configErr).code).toBe(
      SearchErrorCode.SEARCH_UNAVAILABLE,
    );
    expect(toSearchException(serErr).code).toBe(
      SearchErrorCode.SEARCH_UNAVAILABLE,
    );
    expect(toSearchException(deserErr).code).toBe(
      SearchErrorCode.SEARCH_UNAVAILABLE,
    );
    expect(toSearchException(abortErr).code).toBe(
      SearchErrorCode.SEARCH_UNAVAILABLE,
    );
    expect(toSearchException(productErr).code).toBe(
      SearchErrorCode.SEARCH_UNAVAILABLE,
    );
    expect(toSearchException(err400Other).code).toBe(
      SearchErrorCode.SEARCH_UNAVAILABLE,
    );
  });

  it('should rethrow unknown errors as-is', () => {
    const standardError = new Error('Unexpected database glitch');
    expect(() => toSearchException(standardError)).toThrow(standardError);

    const typeError = new TypeError('Cannot read properties of undefined');
    expect(() => toSearchException(typeError)).toThrow(typeError);

    const customObject = { message: 'weird object error' };
    expect(() => toSearchException(customObject)).toThrow();
  });
});
