import { ExecutionContext } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { Test, TestingModule } from '@nestjs/testing';
import { RequestContext } from '../common/context/request-context';
import { ReindexState } from '../common/enums';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { ROLES_KEY, STEP_UP_KEY, RolesGuard, StepUpGuard } from '../security';
import { AdminSearchController } from './admin-search.controller';
import { ReindexRequestDto, ReindexSource } from './dto/reindex-request.dto';
import { ReindexService } from './reindex.service';

describe('AdminSearchController (Unit)', () => {
  let controller: AdminSearchController;
  let reindexService: jest.Mocked<ReindexService>;
  let reflector: Reflector;

  beforeEach(async () => {
    reindexService = {
      getStatus: jest.fn().mockResolvedValue({
        alias: 'products-read',
        active_index: 'products-v1-20261014100000',
        mapping_version: 1,
        document_count: 48213,
        last_event_at: '2026-10-14T12:00:00Z',
        consumer_lag: {},
        dlq_count: 0,
        reindex_state: 'IDLE',
      }),
      start: jest.fn().mockResolvedValue({
        job_id: 'job-01912fb8-7a1b-7c12-9c55-8b1c34a6d001',
        state: ReindexState.REQUESTED,
        target_index: 'products-v1-20261014120000',
      }),
      getJob: jest.fn().mockResolvedValue({
        job_id: 'job-01912fb8-7a1b-7c12-9c55-8b1c34a6d001',
        state: ReindexState.RUNNING,
        target_index: 'products-v1-20261014120000',
        mapping_version: 1,
        processed_count: 500,
        failed_count: 0,
        total_estimate: 1000,
        checkpoint: 'p-1',
        started_at: '2026-10-14T12:00:00Z',
        updated_at: '2026-10-14T12:01:00Z',
        error_code: null,
      }),
      cancel: jest.fn().mockResolvedValue({
        job_id: 'job-01912fb8-7a1b-7c12-9c55-8b1c34a6d001',
        state: ReindexState.CANCELLED,
        target_index: 'products-v1-20261014120000',
        mapping_version: 1,
        processed_count: 500,
        failed_count: 0,
        total_estimate: 1000,
        checkpoint: 'p-1',
        started_at: '2026-10-14T12:00:00Z',
        updated_at: '2026-10-14T12:02:00Z',
        error_code: null,
      }),
    } as unknown as jest.Mocked<ReindexService>;

    const module: TestingModule = await Test.createTestingModule({
      controllers: [AdminSearchController],
      providers: [
        {
          provide: ReindexService,
          useValue: reindexService,
        },
        Reflector,
      ],
    }).compile();

    controller = module.get<AdminSearchController>(AdminSearchController);
    reflector = module.get<Reflector>(Reflector);
  });

  describe('Security Metadata & Guards Inspection (RX-U-01)', () => {
    it('[RX-U-01] AdminSearchController guard setup: @AdminOnly() on controller and @AdminOnly({ stepUp: true }) on mutations', () => {
      // Controller-level roles metadata
      const controllerRoles = reflector.get<string[]>(
        ROLES_KEY,
        AdminSearchController,
      );
      expect(controllerRoles).toContain('SEARCH_ADMIN');

      // POST /reindex requires Step-Up
      const reindexStepUp = reflector.get<boolean>(
        STEP_UP_KEY,
        controller.reindex,
      );
      expect(reindexStepUp).toBe(true);

      // POST /reindex/:jobId/cancel requires Step-Up (D17)
      const cancelStepUp = reflector.get<boolean>(
        STEP_UP_KEY,
        controller.cancel,
      );
      expect(cancelStepUp).toBe(true);

      // Read endpoints do NOT require Step-Up
      const statusStepUp = reflector.get<boolean>(
        STEP_UP_KEY,
        controller.getStatus,
      );
      expect(statusStepUp).toBeUndefined();

      const getJobStepUp = reflector.get<boolean>(
        STEP_UP_KEY,
        controller.getJob,
      );
      expect(getJobStepUp).toBeUndefined();
    });
  });

  describe('Guard Execution with RequestContext (RX-U-02, RX-U-03, RX-U-04)', () => {
    let rolesGuard: RolesGuard;
    let stepUpGuard: StepUpGuard;

    beforeEach(() => {
      rolesGuard = new RolesGuard(reflector);
      stepUpGuard = new StepUpGuard(reflector);
    });

    const createMockExecutionContext = (
      handler: (...args: never[]) => unknown,
    ): ExecutionContext =>
      ({
        getHandler: () => handler as never,
        getClass: () => AdminSearchController,
        switchToHttp: () => ({
          getRequest: () => ({}),
        }),
      }) as unknown as ExecutionContext;

    it('[RX-U-02] User context extractor with empty or missing X-User-Roles throws SEARCH_ADMIN_FORBIDDEN', () => {
      const mockContext = createMockExecutionContext(controller.getStatus);

      const anonymousContext = {
        requestId: 'r1',
        traceId: 't1',
        spanId: 's1',
        traceparent: 'tp1',
        actor: { roles: [], permissions: [], mfaStepUp: false },
      };

      RequestContext.run(anonymousContext, () => {
        expect(() => rolesGuard.canActivate(mockContext)).toThrow(
          SearchException,
        );
        try {
          rolesGuard.canActivate(mockContext);
        } catch (err: unknown) {
          const e = err as SearchException;
          expect(e.code).toBe(SearchErrorCode.SEARCH_ADMIN_FORBIDDEN);
          expect(e.getStatus()).toBe(403);
        }
      });
    });

    it('[RX-U-03] User context extractor with X-User-Roles: BUYER,SELLER throws SEARCH_ADMIN_FORBIDDEN', () => {
      const mockContext = createMockExecutionContext(controller.getStatus);

      const nonAdminContext = {
        requestId: 'r1',
        traceId: 't1',
        spanId: 's1',
        traceparent: 'tp1',
        actor: {
          roles: ['BUYER', 'SELLER'],
          permissions: [],
          mfaStepUp: false,
        },
      };

      RequestContext.run(nonAdminContext, () => {
        expect(() => rolesGuard.canActivate(mockContext)).toThrow(
          SearchException,
        );
        try {
          rolesGuard.canActivate(mockContext);
        } catch (err: unknown) {
          const e = err as SearchException;
          expect(e.code).toBe(SearchErrorCode.SEARCH_ADMIN_FORBIDDEN);
        }
      });
    });

    it('[RX-U-04] User context with X-User-Roles: SEARCH_ADMIN but StepUpGuard encounters empty X-MFA-Step-Up throws SEARCH_ADMIN_FORBIDDEN', () => {
      const mockContext = createMockExecutionContext(controller.reindex);

      const adminWithoutStepUp = {
        requestId: 'r1',
        traceId: 't1',
        spanId: 's1',
        traceparent: 'tp1',
        actor: {
          roles: ['SEARCH_ADMIN'],
          permissions: [],
          mfaStepUp: false,
        },
      };

      RequestContext.run(adminWithoutStepUp, () => {
        expect(rolesGuard.canActivate(mockContext)).toBe(true);
        expect(() => stepUpGuard.canActivate(mockContext)).toThrow(
          SearchException,
        );
        try {
          stepUpGuard.canActivate(mockContext);
        } catch (err: unknown) {
          const e = err as SearchException;
          expect(e.code).toBe(SearchErrorCode.SEARCH_ADMIN_FORBIDDEN);
        }
      });
    });
  });

  describe('Endpoint Invocations & Envelope Wrappers', () => {
    it('getStatus() invokes service and wraps result in standard envelope', async () => {
      const result = await controller.getStatus({});
      expect(reindexService.getStatus).toHaveBeenCalled();
      expect(result.data).toBeDefined();
      expect(result.data.alias).toBe('products-read');
      expect(result.meta).toBeDefined();
    });

    it('reindex() invokes start() with dto and returns 202 descriptor in data', async () => {
      const dto: ReindexRequestDto = {
        source: ReindexSource.INDEX_COPY,
        mapping_version: 1,
        reason: 'Rebuild index',
      };

      const result = await controller.reindex(dto);
      expect(reindexService.start).toHaveBeenCalledWith(dto);
      expect(result.data.job_id).toBeDefined();
      expect(result.data.state).toBe(ReindexState.REQUESTED);
    });

    it('getJob() invokes getJob() with jobId and returns job progress', async () => {
      const jobId = 'job-01912fb8-7a1b-7c12-9c55-8b1c34a6d001';
      const result = await controller.getJob(jobId);
      expect(reindexService.getJob).toHaveBeenCalledWith(jobId);
      expect(result.data.job_id).toBe(jobId);
      expect(result.data.state).toBe(ReindexState.RUNNING);
    });

    it('cancel() invokes cancel() with jobId and returns cancelled job', async () => {
      const jobId = 'job-01912fb8-7a1b-7c12-9c55-8b1c34a6d001';
      const result = await controller.cancel(jobId);
      expect(reindexService.cancel).toHaveBeenCalledWith(jobId);
      expect(result.data.job_id).toBe(jobId);
      expect(result.data.state).toBe(ReindexState.CANCELLED);
    });
  });
});
