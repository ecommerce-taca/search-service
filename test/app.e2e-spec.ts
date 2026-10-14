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
  ERROR_CODE_NOT_FOUND,
  ERROR_MESSAGES_MAP,
  SearchErrorCode,
} from '../src/common/errors/error-codes';
import { SearchExceptionFilter } from '../src/common/errors/search-exception.filter';
import { IndexBootstrapService } from '../src/product-index/index-bootstrap.service';
import { AdminOnly } from '../src/security';

@Controller('admin-test')
class TestAdminController {
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

describe('App (e2e)', () => {
  let app: INestApplication;

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
      controllers: [TestAdminController],
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

  it('GET /health/live returns 200 with UP status, service name, time, and meta', async () => {
    const res = await request(app.getHttpServer())
      .get('/health/live')
      .expect(200);

    expect(res.body).toEqual({
      data: {
        status: 'UP',
        service: 'search-service',
        time: expect.any(String),
      },
      meta: {
        request_id: expect.any(String),
      },
    });
  });

  it('GET /health/live echoes X-Request-ID response header and inside meta.request_id', async () => {
    const customId = 'e2e-request-id-12345';
    const res = await request(app.getHttpServer())
      .get('/health/live')
      .set('X-Request-ID', customId)
      .expect(200);

    expect(res.headers['x-request-id']).toBe(customId);
    expect(res.body.meta.request_id).toBe(customId);
  });

  it('GET /metrics returns 200 with Prometheus metrics', async () => {
    const res = await request(app.getHttpServer()).get('/metrics').expect(200);

    expect(res.text).toContain('http_request_duration_seconds');
  });

  it('GET /unknown-route returns 404 with error envelope and echoes X-Request-ID', async () => {
    const customId = 'unknown-route-req-id-777';
    const res = await request(app.getHttpServer())
      .get('/api/v1/unknown-endpoint-path')
      .set('X-Request-ID', customId)
      .expect(404);

    expect(res.headers['x-request-id']).toBe(customId);
    expect(res.body).toEqual({
      error: {
        code: ERROR_CODE_NOT_FOUND,
        message: 'Không tìm thấy tài nguyên.',
        details: null,
        trace_id: expect.any(String),
      },
    });
  });

  describe('Security Global Guards & AdminOnly (e2e)', () => {
    it('GET /api/v1/admin-test/basic returns 403 SEARCH_ADMIN_FORBIDDEN for anonymous', async () => {
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

    it('GET /api/v1/admin-test/step-up returns 403 for admin without step-up', async () => {
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

    it('GET /api/v1/admin-test/step-up returns 200 for admin with X-MFA-Step-Up: true', async () => {
      const res = await request(app.getHttpServer())
        .get('/api/v1/admin-test/step-up')
        .set('x-user-roles', 'SEARCH_ADMIN')
        .set('x-mfa-step-up', 'true')
        .expect(200);

      expect(res.body).toEqual({ status: 'step-up-ok' });
    });
  });
});
