import { log } from '@core/log/log.config';
import { take } from 'rxjs/operators';
import { DbAccessor, DbSchema } from '@core/database/db-accessor';
import { Observable } from 'rxjs/internal/Observable';
import { Injectable } from '@angular/core';

@Injectable()
export abstract class AbstractIndexedDbAccessor implements DbAccessor {
  public static readonly DATABASE_VERSION = '85';

  static getVersion(): number {
    return parseFloat(localStorage.getItem('databaseVersion'));
  }

  abstract deleteDB(): Observable<any>;

  abstract ready(): Observable<any>;

  abstract init(schema: DbSchema.Schema): Observable<any>;

  protected checkDatabaseVersion(): void {
    if (localStorage.getItem('databaseVersion') == null ) {
      localStorage.setItem('databaseVersion', AbstractIndexedDbAccessor.DATABASE_VERSION);
      log.info('Enregistrement de la version de la BDD : ' + AbstractIndexedDbAccessor.DATABASE_VERSION);
    } else if (localStorage.getItem('databaseVersion') !== AbstractIndexedDbAccessor.DATABASE_VERSION) {
      localStorage.setItem('databaseVersion', AbstractIndexedDbAccessor.DATABASE_VERSION);
      // Suppression de la bdd locale
      this.deleteDB().subscribe(take(1));
    }
  }
}
