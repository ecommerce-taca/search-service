import { Controller, Get } from '@nestjs/common';
import { ApiResponse, createResponse } from '../common/response.helper';
import { ReadinessReport, ReadinessService } from './readiness.service';

@Controller('search')
export class ReadinessController {
  constructor(private readonly readinessService: ReadinessService) {}

  @Get('health')
  async check(): Promise<ApiResponse<ReadinessReport>> {
    const report = await this.readinessService.check();
    return createResponse(report);
  }
}
