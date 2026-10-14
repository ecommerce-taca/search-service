import { Test, TestingModule } from '@nestjs/testing';
import { AppConfigService } from '../config/config.service';
import { HealthController } from './health.controller';

describe('HealthController (Unit)', () => {
  let controller: HealthController;
  let mockConfigService: {
    serviceName: string;
  };

  beforeEach(async () => {
    mockConfigService = {
      serviceName: 'search-service',
    };

    const module: TestingModule = await Test.createTestingModule({
      controllers: [HealthController],
      providers: [
        {
          provide: AppConfigService,
          useValue: mockConfigService,
        },
      ],
    }).compile();

    controller = module.get<HealthController>(HealthController);
  });

  describe('getLive', () => {
    it('HL-U-05: returns UP liveness status decoupled from external ES connectivity', () => {
      const response = controller.getLive();

      expect(response.data).toEqual({
        status: 'UP',
        service: 'search-service',
        time: expect.any(String),
      });
      expect(response.meta).toBeDefined();
    });
  });
});
