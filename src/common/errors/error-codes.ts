import { HttpStatus } from '@nestjs/common';

export const ERROR_CODE_NOT_FOUND = 'NOT_FOUND';

export enum SearchErrorCode {
  SEARCH_INVALID_INPUT = 'SEARCH_INVALID_INPUT',
  SEARCH_QUERY_TOO_LONG = 'SEARCH_QUERY_TOO_LONG',
  SEARCH_UNAVAILABLE = 'SEARCH_UNAVAILABLE',
  SEARCH_TIMEOUT = 'SEARCH_TIMEOUT',
  SEARCH_INDEX_NOT_READY = 'SEARCH_INDEX_NOT_READY',
  SEARCH_ADMIN_FORBIDDEN = 'SEARCH_ADMIN_FORBIDDEN',
  SEARCH_REINDEX_CONFLICT = 'SEARCH_REINDEX_CONFLICT',
  SEARCH_REINDEX_NOT_FOUND = 'SEARCH_REINDEX_NOT_FOUND',
  SEARCH_EVENT_INVALID = 'SEARCH_EVENT_INVALID',
  SEARCH_EVENT_VERSION_OLD = 'SEARCH_EVENT_VERSION_OLD',
  SEARCH_INTERNAL_ERROR = 'SEARCH_INTERNAL_ERROR',
}

export const ERROR_HTTP_STATUS_MAP: Record<SearchErrorCode, number> = {
  [SearchErrorCode.SEARCH_INVALID_INPUT]: HttpStatus.BAD_REQUEST, // 400
  [SearchErrorCode.SEARCH_QUERY_TOO_LONG]: HttpStatus.BAD_REQUEST, // 400
  [SearchErrorCode.SEARCH_ADMIN_FORBIDDEN]: HttpStatus.FORBIDDEN, // 403
  [SearchErrorCode.SEARCH_REINDEX_NOT_FOUND]: HttpStatus.NOT_FOUND, // 404
  [SearchErrorCode.SEARCH_REINDEX_CONFLICT]: HttpStatus.CONFLICT, // 409
  [SearchErrorCode.SEARCH_EVENT_INVALID]: HttpStatus.BAD_REQUEST, // 400 / internal
  [SearchErrorCode.SEARCH_EVENT_VERSION_OLD]: HttpStatus.OK, // 200 / internal
  [SearchErrorCode.SEARCH_INDEX_NOT_READY]: HttpStatus.SERVICE_UNAVAILABLE, // 503
  [SearchErrorCode.SEARCH_UNAVAILABLE]: HttpStatus.SERVICE_UNAVAILABLE, // 503
  [SearchErrorCode.SEARCH_TIMEOUT]: HttpStatus.GATEWAY_TIMEOUT, // 504
  [SearchErrorCode.SEARCH_INTERNAL_ERROR]: HttpStatus.INTERNAL_SERVER_ERROR, // 500
};

export const ERROR_MESSAGES_MAP: Record<SearchErrorCode, string> = {
  [SearchErrorCode.SEARCH_INVALID_INPUT]: 'Tham số tìm kiếm chưa hợp lệ.',
  [SearchErrorCode.SEARCH_QUERY_TOO_LONG]: 'Từ khóa tìm kiếm quá dài.',
  [SearchErrorCode.SEARCH_UNAVAILABLE]:
    'Hệ thống tìm kiếm tạm thời không khả dụng.',
  [SearchErrorCode.SEARCH_TIMEOUT]: 'Tìm kiếm mất quá nhiều thời gian.',
  [SearchErrorCode.SEARCH_INDEX_NOT_READY]:
    'Dữ liệu tìm kiếm đang được khởi tạo.',
  [SearchErrorCode.SEARCH_ADMIN_FORBIDDEN]:
    'Bạn không có quyền quản trị tìm kiếm.',
  [SearchErrorCode.SEARCH_REINDEX_CONFLICT]:
    'Đang có tiến trình đồng bộ tìm kiếm.',
  [SearchErrorCode.SEARCH_REINDEX_NOT_FOUND]:
    'Không tìm thấy tiến trình đồng bộ.',
  [SearchErrorCode.SEARCH_EVENT_INVALID]: 'Sự kiện đồng bộ không hợp lệ.',
  [SearchErrorCode.SEARCH_EVENT_VERSION_OLD]:
    'Sự kiện cũ bị bỏ qua, ghi metric.',
  [SearchErrorCode.SEARCH_INTERNAL_ERROR]:
    'Hệ thống đang bận. Vui lòng thử lại sau.',
};
