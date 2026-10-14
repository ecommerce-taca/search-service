import { Global, Module } from '@nestjs/common';
import { EsCategoryProjection } from './category-projection.service';
import { ProductDocumentBuilder } from './document-builder/product-document.builder';
import { ElasticsearchModule } from './elasticsearch.module';
import { IndexBootstrapService } from './index-bootstrap.service';
import {
  CATEGORY_PROJECTION,
  INDEX_WRITER,
  PRODUCT_DOCUMENT_BUILDER,
} from './interfaces';
import { CategoryProjectionWriter } from './projection/category-projection.writer';
import { VariantAggregator } from './sku/variant-aggregator';
import { EsIndexWriter } from './writer/es-index-writer';

@Global()
@Module({
  imports: [ElasticsearchModule],
  providers: [
    IndexBootstrapService,
    {
      provide: CATEGORY_PROJECTION,
      useClass: EsCategoryProjection,
    },
    EsCategoryProjection,
    ProductDocumentBuilder,
    {
      provide: PRODUCT_DOCUMENT_BUILDER,
      useClass: ProductDocumentBuilder,
    },
    EsIndexWriter,
    {
      provide: INDEX_WRITER,
      useClass: EsIndexWriter,
    },
    VariantAggregator,
    CategoryProjectionWriter,
  ],
  exports: [
    ElasticsearchModule,
    IndexBootstrapService,
    CATEGORY_PROJECTION,
    EsCategoryProjection,
    PRODUCT_DOCUMENT_BUILDER,
    ProductDocumentBuilder,
    INDEX_WRITER,
    EsIndexWriter,
    VariantAggregator,
    CategoryProjectionWriter,
  ],
})
export class ProductIndexModule {}
