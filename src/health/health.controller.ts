import { Controller, Get } from '@nestjs/common';
import { ApiResponse, createResponse } from '../common/response.helper';
import { AppConfigService } from '../config/config.service';

export interface LivenessStatus {
  status: 'UP';
  service: string;
  time: string;
}

@Controller('health')
export class HealthController {
  constructor(private readonly configService: AppConfigService) {}

  @Get('live')
  getLive(): ApiResponse<LivenessStatus> {
    return createResponse({
      status: 'UP',
      service: this.configService.serviceName,
      time: new Date().toISOString(),
    });
  }
}
