import { Injectable } from '@nestjs/common';
import { CategoryProjectionWriter } from '../../product-index/projection/category-projection.writer';
import { DomainEvent } from '../interfaces';

@Injectable()
export class CategoryHandler {
  constructor(
    private readonly categoryProjectionWriter: CategoryProjectionWriter,
  ) {}

  async handle(evt: DomainEvent<unknown>): Promise<void> {
    await this.categoryProjectionWriter.handleCategoryEvent(evt);
  }
}
