import { ExecutionContext } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { RequestContext } from '../common/context/request-context';
import { SearchErrorCode } from '../common/errors/error-codes';
import { SearchException } from '../common/errors/search.exception';
import { RolesGuard } from './roles.guard';
import { StepUpGuard } from './step-up.guard';

describe('Security Guards', () => {
  let reflector: Reflector;
  let context: ExecutionContext;

  beforeEach(() => {
    reflector = new Reflector();
    context = {
      getHandler: () => ({}),
      getClass: () => ({}),
    } as unknown as ExecutionContext;
  });

  describe('RolesGuard', () => {
    it('should allow access if no roles are required', () => {
      jest.spyOn(reflector, 'getAllAndOverride').mockReturnValue(undefined);
      const guard = new RolesGuard(reflector);

      expect(guard.canActivate(context)).toBe(true);
    });

    it('should allow access if user has required SEARCH_ADMIN role', () => {
      jest
        .spyOn(reflector, 'getAllAndOverride')
        .mockReturnValue(['SEARCH_ADMIN']);
      const guard = new RolesGuard(reflector);

      RequestContext.run(
        {
          requestId: 'test-req',
          traceId: 'tid',
          spanId: 'sid',
          traceparent: '00-tid-sid-01',
          actor: {
            roles: ['SEARCH_ADMIN', 'USER'],
            permissions: [],
            mfaStepUp: false,
          },
        },
        () => {
          expect(guard.canActivate(context)).toBe(true);
        },
      );
    });

    it('should throw SEARCH_ADMIN_FORBIDDEN 403 if user lacks SEARCH_ADMIN role', () => {
      jest
        .spyOn(reflector, 'getAllAndOverride')
        .mockReturnValue(['SEARCH_ADMIN']);
      const guard = new RolesGuard(reflector);

      RequestContext.run(
        {
          requestId: 'test-req',
          traceId: 'tid',
          spanId: 'sid',
          traceparent: '00-tid-sid-01',
          actor: { roles: ['USER'], permissions: [], mfaStepUp: false },
        },
        () => {
          try {
            guard.canActivate(context);
            fail('Expected SearchException to be thrown');
          } catch (err) {
            expect(err).toBeInstanceOf(SearchException);
            const exc = err as SearchException;
            expect(exc.code).toBe(SearchErrorCode.SEARCH_ADMIN_FORBIDDEN);
            expect(exc.getStatus()).toBe(403);
          }
        },
      );
    });

    it.each([
      [
        'SEARCH_ADMIN permission from auth-user',
        { roles: ['SELLER'], permissions: ['SEARCH_ADMIN'] },
      ],
      [
        'SUPER_ADMIN role from auth-user',
        { roles: ['SUPER_ADMIN'], permissions: [] },
      ],
    ])('should allow access for %s', (_label, actor) => {
      jest
        .spyOn(reflector, 'getAllAndOverride')
        .mockReturnValue(['SEARCH_ADMIN']);
      const guard = new RolesGuard(reflector);

      RequestContext.run(
        {
          requestId: 'test-req',
          traceId: 'tid',
          spanId: 'sid',
          traceparent: '00-tid-sid-01',
          actor: { ...actor, mfaStepUp: false },
        },
        () => {
          expect(guard.canActivate(context)).toBe(true);
        },
      );
    });
  });

  describe('StepUpGuard', () => {
    it('should allow access if step up is not required', () => {
      jest.spyOn(reflector, 'getAllAndOverride').mockReturnValue(undefined);
      const guard = new StepUpGuard(reflector);

      expect(guard.canActivate(context)).toBe(true);
    });

    it('should allow access if user has completed step up MFA', () => {
      jest.spyOn(reflector, 'getAllAndOverride').mockReturnValue(true);
      const guard = new StepUpGuard(reflector);

      RequestContext.run(
        {
          requestId: 'test-req',
          traceId: 'tid',
          spanId: 'sid',
          traceparent: '00-tid-sid-01',
          actor: { roles: ['SEARCH_ADMIN'], permissions: [], mfaStepUp: true },
        },
        () => {
          expect(guard.canActivate(context)).toBe(true);
        },
      );
    });

    it('should throw SEARCH_ADMIN_FORBIDDEN 403 if user lacks step up MFA', () => {
      jest.spyOn(reflector, 'getAllAndOverride').mockReturnValue(true);
      const guard = new StepUpGuard(reflector);

      RequestContext.run(
        {
          requestId: 'test-req',
          traceId: 'tid',
          spanId: 'sid',
          traceparent: '00-tid-sid-01',
          actor: { roles: ['SEARCH_ADMIN'], permissions: [], mfaStepUp: false },
        },
        () => {
          try {
            guard.canActivate(context);
            fail('Expected SearchException to be thrown');
          } catch (err) {
            expect(err).toBeInstanceOf(SearchException);
            const exc = err as SearchException;
            expect(exc.code).toBe(SearchErrorCode.SEARCH_ADMIN_FORBIDDEN);
            expect(exc.getStatus()).toBe(403);
          }
        },
      );
    });
  });
});
