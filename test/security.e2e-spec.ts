import {
  Controller,
  Get,
  INestApplication,
  ValidationPipe,
} from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import request from 'supertest';
import { AppModule } from '../src/app.module';
import {
  ERROR_MESSAGES_MAP,
  SearchErrorCode,
} from '../src/common/errors/error-codes';
import { SearchExceptionFilter } from '../src/common/errors/search-exception.filter';
import { IndexBootstrapService } from '../src/product-index/index-bootstrap.service';
import { AdminOnly } from '../src/security';

@Controller('admin-test')
class TestAdminSecurityController {
  @Get('basic')
  @AdminOnly()
  getBasic() {
    return { status: 'admin-ok' };
  }

  @Get('step-up')
  @AdminOnly({ stepUp: true })
  getStepUp() {
    return { status: 'step-up-ok' };
  }
}

describe('Security Acceptance (D15) (e2e)', () => {
  let app: INestApplication;

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
      controllers: [TestAdminSecurityController],
    })
      .overrideProvider(IndexBootstrapService)
      .useValue({
        onApplicationBootstrap: jest.fn().mockResolvedValue(undefined),
        bootstrap: jest.fn().mockResolvedValue({
          alias: 'products-read',
          aliasCreated: false,
          consumerStateIndex: 'search-consumer-state',
          consumerStateCreated: false,
          reindexJobsIndex: 'search-reindex-jobs',
          reindexJobsCreated: false,
          searchCategoriesIndex: 'search-categories',
          searchCategoriesCreated: false,
        }),
      })
      .compile();

    app = moduleFixture.createNestApplication();

    app.setGlobalPrefix('api/v1', {
      exclude: ['health/live', 'metrics'],
    });

    app.useGlobalFilters(new SearchExceptionFilter());

    app.useGlobalPipes(
      new ValidationPipe({
        whitelist: true,
        transform: true,
        forbidNonWhitelisted: true,
      }),
    );

    await app.init();
  });

  afterAll(async () => {
    await app.close();
  });

  it('SEC-E-01: anonymous → 403 SEARCH_ADMIN_FORBIDDEN', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/admin-test/basic')
      .expect(403);

    expect(res.body).toEqual({
      error: {
        code: SearchErrorCode.SEARCH_ADMIN_FORBIDDEN,
        message: ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_ADMIN_FORBIDDEN],
        details: null,
        trace_id: expect.any(String),
      },
    });
  });

  it('SEC-E-02: role SELLER → 403 SEARCH_ADMIN_FORBIDDEN', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/admin-test/basic')
      .set('x-user-roles', 'SELLER')
      .expect(403);

    expect(res.body).toEqual({
      error: {
        code: SearchErrorCode.SEARCH_ADMIN_FORBIDDEN,
        message: ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_ADMIN_FORBIDDEN],
        details: null,
        trace_id: expect.any(String),
      },
    });
  });

  it('SEC-E-02: role BUYER → 403 SEARCH_ADMIN_FORBIDDEN', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/admin-test/basic')
      .set('x-user-roles', 'BUYER')
      .expect(403);

    expect(res.body).toEqual({
      error: {
        code: SearchErrorCode.SEARCH_ADMIN_FORBIDDEN,
        message: ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_ADMIN_FORBIDDEN],
        details: null,
        trace_id: expect.any(String),
      },
    });
  });

  it('SEC-E-03: SEARCH_ADMIN on basic route without step-up → 200 OK', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/admin-test/basic')
      .set('x-user-roles', 'SEARCH_ADMIN')
      .expect(200);

    expect(res.body).toEqual({ status: 'admin-ok' });
  });

  it('SEC-E-04: SEARCH_ADMIN on stepUp route without header → 403 SEARCH_ADMIN_FORBIDDEN', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/admin-test/step-up')
      .set('x-user-roles', 'SEARCH_ADMIN')
      .expect(403);

    expect(res.body).toEqual({
      error: {
        code: SearchErrorCode.SEARCH_ADMIN_FORBIDDEN,
        message: ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_ADMIN_FORBIDDEN],
        details: null,
        trace_id: expect.any(String),
      },
    });
  });

  it('SEC-E-05: SEARCH_ADMIN on stepUp route with X-MFA-Step-Up: false → 403 SEARCH_ADMIN_FORBIDDEN', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/admin-test/step-up')
      .set('x-user-roles', 'SEARCH_ADMIN')
      .set('x-mfa-step-up', 'false')
      .expect(403);

    expect(res.body).toEqual({
      error: {
        code: SearchErrorCode.SEARCH_ADMIN_FORBIDDEN,
        message: ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_ADMIN_FORBIDDEN],
        details: null,
        trace_id: expect.any(String),
      },
    });
  });

  it('SEC-E-06: SEARCH_ADMIN on stepUp route with X-MFA-Step-Up: true → 200', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/admin-test/step-up')
      .set('x-user-roles', 'SEARCH_ADMIN')
      .set('x-mfa-step-up', 'true')
      .expect(200);

    expect(res.body).toEqual({ status: 'step-up-ok' });
  });

  it('SEC-E-06: SEARCH_ADMIN on stepUp route with X-MFA-Step-Up: 1 → 200', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/admin-test/step-up')
      .set('x-user-roles', 'SEARCH_ADMIN')
      .set('x-mfa-step-up', '1')
      .expect(200);

    expect(res.body).toEqual({ status: 'step-up-ok' });
  });

  it('spoofed role casing search_admin → 403 SEARCH_ADMIN_FORBIDDEN', async () => {
    const res = await request(app.getHttpServer())
      .get('/api/v1/admin-test/basic')
      .set('x-user-roles', 'search_admin')
      .expect(403);

    expect(res.body).toEqual({
      error: {
        code: SearchErrorCode.SEARCH_ADMIN_FORBIDDEN,
        message: ERROR_MESSAGES_MAP[SearchErrorCode.SEARCH_ADMIN_FORBIDDEN],
        details: null,
        trace_id: expect.any(String),
      },
    });
  });
});
