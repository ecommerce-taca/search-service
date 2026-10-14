import { INestApplication, ValidationPipe } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import request from 'supertest';
import { AppModule } from '../src/app.module';
import { SearchExceptionFilter } from '../src/common/errors/search-exception.filter';

describe('App (e2e)', () => {
  let app: INestApplication;

  beforeAll(async () => {
    process.env.ES_NODE = process.env.ES_NODE || 'http://localhost:9200';
    process.env.KAFKA_BROKERS = process.env.KAFKA_BROKERS || 'localhost:9092';

    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

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
        code: 'NOT_FOUND',
        message: expect.stringContaining('Cannot GET'),
        details: null,
        trace_id: expect.any(String),
      },
    });
  });
});
