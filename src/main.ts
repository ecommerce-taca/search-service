import 'reflect-metadata';
import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { Logger } from 'nestjs-pino';
import { AppModule } from './app.module';
import { SearchExceptionFilter } from './common/errors/search-exception.filter';
import { AppConfigService } from './config/config.service';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, { bufferLogs: true });

  const logger = app.get(Logger);
  app.useLogger(logger);

  const configService = app.get(AppConfigService);

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

  app.enableShutdownHooks();

  const port = configService.port;
  await app.listen(port);
  logger.log({
    event: 'app_start',
    port,
    message: `Application listening on port ${port}`,
  });
}

void bootstrap();
