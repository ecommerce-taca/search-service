import { Client } from '@elastic/elasticsearch';
import { Global, Module, OnApplicationShutdown } from '@nestjs/common';
import { ES_REQUEST_TIMEOUT_MS } from '../common/constants';
import { AppConfigModule } from '../config/config.module';
import { AppConfigService } from '../config/config.service';

export const ELASTICSEARCH_CLIENT = 'ELASTICSEARCH_CLIENT';

@Global()
@Module({
  imports: [AppConfigModule],
  providers: [
    {
      provide: ELASTICSEARCH_CLIENT,
      inject: [AppConfigService],
      useFactory: (config: AppConfigService) => {
        return new Client({
          node: config.esNode,
          requestTimeout: ES_REQUEST_TIMEOUT_MS,
        });
      },
    },
    {
      provide: Client,
      useExisting: ELASTICSEARCH_CLIENT,
    },
  ],
  exports: [ELASTICSEARCH_CLIENT, Client],
})
export class ElasticsearchModule implements OnApplicationShutdown {
  constructor(private readonly client: Client) {}

  async onApplicationShutdown() {
    await this.client.close();
  }
}
