import { Reflector } from '@nestjs/core';
import { ROLES_KEY, STEP_UP_KEY } from '../security';
import { AdminSearchController } from './admin-search.controller';
import { ReindexService } from './reindex.service';

describe('AdminSearchController', () => {
  let controller: AdminSearchController;
  let mockReindexService: jest.Mocked<Partial<ReindexService>>;
  const reflector = new Reflector();

  beforeEach(() => {
    mockReindexService = {
      getStatus: jest.fn(),
      start: jest.fn(),
      getJob: jest.fn(),
      cancel: jest.fn(),
    };
    controller = new AdminSearchController(
      mockReindexService as ReindexService,
    );
  });

  describe('guard setup', () => {
    it('attaches SEARCH_ADMIN role to all controller endpoints', () => {
      const statusRoles = reflector.get<string[]>(
        ROLES_KEY,
        controller.getStatus,
      );
      expect(statusRoles).toContain('SEARCH_ADMIN');

      const reindexRoles = reflector.get<string[]>(
        ROLES_KEY,
        controller.reindex,
      );
      expect(reindexRoles).toContain('SEARCH_ADMIN');

      const getJobRoles = reflector.get<string[]>(
        ROLES_KEY,
        controller.getReindexJob,
      );
      expect(getJobRoles).toContain('SEARCH_ADMIN');

      const cancelRoles = reflector.get<string[]>(
        ROLES_KEY,
        controller.cancelReindexJob,
      );
      expect(cancelRoles).toContain('SEARCH_ADMIN');
    });

    it('attaches stepUp required to mutation endpoints (reindex, cancel)', () => {
      const reindexStepUp = reflector.get<boolean>(
        STEP_UP_KEY,
        controller.reindex,
      );
      expect(reindexStepUp).toBe(true);

      const cancelStepUp = reflector.get<boolean>(
        STEP_UP_KEY,
        controller.cancelReindexJob,
      );
      expect(cancelStepUp).toBe(true);

      const statusStepUp = reflector.get<boolean>(
        STEP_UP_KEY,
        controller.getStatus,
      );
      expect(statusStepUp).toBeFalsy();

      const getJobStepUp = reflector.get<boolean>(
        STEP_UP_KEY,
        controller.getReindexJob,
      );
      expect(getJobStepUp).toBeFalsy();
    });
  });
});
