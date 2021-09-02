import { ConnectedDatabase, DbQuery, DbSchema } from '@core/database/db-accessor';
import * as lf from 'lovefield';
import { Observable } from 'rxjs/internal/Observable';
import { from } from 'rxjs/internal/observable/from';
import { map, mergeMap, take } from 'rxjs/internal/operators';
import { of, ReplaySubject, throwError } from 'rxjs';
import { catchError, tap } from 'rxjs/operators';
import { DeviceService } from '@core/service/device.service';
import { AbstractIndexedDbAccessor } from '@core/database/indexed-db-accessor';
import { log } from '@core/log/log.config';
import { Injectable } from '@angular/core';
import { Mapper } from '@core/utils/mapper.util';
import DataStoreType = lf.schema.DataStoreType;
import TableBuilder = lf.schema.TableBuilder;
import { environment } from '@env';

export namespace LovefieldAccessorQuery {
  class ErrorUtil {
    private  constructor() {
    }

    public static checkparamNonNull(param: any, paramName: string) {
      if (param === null || param === undefined) {
        throw new DbQuery.DbError('Invalid param ' + paramName + ' provided ' + param);
      }
    }
  }

  export class Table<T> implements DbQuery.Table<T> {
    table: lf.schema.Table;

    constructor(private db: lf.Database, table: string|DbSchema.TableDefinition<T>) {
      ErrorUtil.checkparamNonNull(db, 'db');
      ErrorUtil.checkparamNonNull(table, 'table');
      this.table = db.getSchema().table(table instanceof DbSchema.TableDefinition ? table.name : table);
    }

    name(): string {
      return this.table.getName();
    }

    column(columnName: (keyof T & string)): Column<T> {
      if (!this.table[columnName]) {
        throw new DbQuery.DbError('Column ' + columnName + ' doesn\'t exist on table ' + this.table.getName());
      }
      return new Column<T>(this.table[columnName]);
    }

    delete(): Delete<T> {
      return new Delete(this.db, this.table);
    }

    insert(): Insert<T> {
      return new Insert(this.db, this.table);
    }

    insertOrReplace(): Insert<T> {
      return new InsertOrReplace(this.db, this.table);
    }

    select(...columns: (keyof T & string)[]): Select<T> {
      ErrorUtil.checkparamNonNull(columns, 'columns');
      return new Select<T>(this.db, this, ...columns.map(column => this.column(column)));
    }

    update(): Update<T> {
      return new Update<T>(this.db, this.table);
    }
  }

  export class Column<T> implements DbQuery.Column<T> {
    constructor(public column: lf.schema.Column) {
    }

    between(fromValue: DbQuery.ValueLiteral, to: DbQuery.ValueLiteral): DbQuery.Predicate<T> {
      ErrorUtil.checkparamNonNull(fromValue, 'fromValue');
      ErrorUtil.checkparamNonNull(to, 'to');
      return new Predicate<T>(this.column.between(fromValue, to));
    }

    eq(operand: DbQuery.ValueLiteral): DbQuery.Predicate<T> {
      ErrorUtil.checkparamNonNull(operand, 'operand');
      return new Predicate<T>(this.column.eq(operand));
    }

    gt(operand: DbQuery.ValueLiteral): DbQuery.Predicate<T> {
      ErrorUtil.checkparamNonNull(operand, 'operand');
      return new Predicate<T>(this.column.gt(operand));
    }

    gte(operand: DbQuery.ValueLiteral): DbQuery.Predicate<T> {
      ErrorUtil.checkparamNonNull(operand, 'operand');
      return new Predicate<T>(this.column.gte(operand));
    }

    in(values: DbQuery.ValueLiteral[]): DbQuery.Predicate<T> {
      ErrorUtil.checkparamNonNull(values, 'values');
      return new Predicate<T>(this.column.in(values));
    }

    isNotNull(): DbQuery.Predicate<T> {
      return new Predicate<T>(this.column.isNotNull());
    }

    isNull(): DbQuery.Predicate<T> {
      return new Predicate<T>(this.column.isNull());
    }

    lt(operand: DbQuery.ValueLiteral): DbQuery.Predicate<T> {
      ErrorUtil.checkparamNonNull(operand, 'operand');
      return new Predicate<T>(this.column.lt(operand));
    }

    lte(operand: DbQuery.ValueLiteral): DbQuery.Predicate<T> {
      ErrorUtil.checkparamNonNull(operand, 'operand');
      return new Predicate<T>(this.column.lte(operand));
    }

    contains(operand: string): DbQuery.Predicate<T> {
      ErrorUtil.checkparamNonNull(operand, 'operand');
      return new Predicate<T>(this.column.match(new RegExp(operand + '*', 'g')));
    }

    startWith(operand: string): DbQuery.Predicate<T> {
      ErrorUtil.checkparamNonNull(operand, 'operand');
      return new Predicate<T>(new RegExp(operand + '$'));
    }

    endWith(operand: string): DbQuery.Predicate<T> {
      ErrorUtil.checkparamNonNull(operand, 'operand');
      return new Predicate<T>(this.column.match(new RegExp('^' + operand)));
    }

    match(operand: RegExp): DbQuery.Predicate<T> {
      ErrorUtil.checkparamNonNull(operand, 'operand');
      return new Predicate<T>(this.column.match(operand));
    }

    neq(operand: DbQuery.ValueLiteral, skipCheckNotNull = false): DbQuery.Predicate<T> {
      if (!skipCheckNotNull) {
        ErrorUtil.checkparamNonNull(operand, 'operand');
      }
      return new Predicate(this.column.neq(operand));
    }
  }

  export class Predicate<T> implements DbQuery.Predicate<T> {
    constructor(protected predicate: lf.Predicate) {
    }

    get(): lf.Predicate {
      return this.predicate;
    }
  }

  export class Builder implements DbQuery.Builder {
    query: lf.query.Builder;

    exec(): Observable<any> {
      if (environment.showSql) {
        log.debug(this.query.toSql());
      }
      return from(this.query.exec()).pipe(take(1));
    }
  }

  export abstract class BasicSelect<T> extends Builder {
    query: lf.query.Select;
    protected db: lf.Database;
    protected table: Table<T>;
    protected resultMapping: (input: T) => T = input => input;

    limit(numberOfRows: number): this {
      ErrorUtil.checkparamNonNull(numberOfRows, 'numberOfRows');
      this.query = this.query.limit(numberOfRows);
      return this;
    }

    orderBy(column: Column<T>, order?: DbQuery.Order): this {
      ErrorUtil.checkparamNonNull(column, 'column');
      const lfOrder = order === DbQuery.Order.ASC ? lf.Order.ASC : lf.Order.DESC;
      this.query = this.query.orderBy(column.column, lfOrder);
      return this;
    }

    skip(numberOfRows: number): this {
      ErrorUtil.checkparamNonNull(numberOfRows, 'numberOfRows');
      this.query = this.query.skip(numberOfRows);
      return this;
    }

    where(predicate: Predicate<T>): this {
      ErrorUtil.checkparamNonNull(predicate, 'predicate');
      this.query = this.query.where(predicate.get());
      return this;
    }

    mapFrom(source: T): this {
      ErrorUtil.checkparamNonNull(source, 'source');
      this.resultMapping = (input: T) => Mapper.mapFromDatabase(input, source);
      return this;
    }
  }

  export class Select<T> extends BasicSelect<T> implements DbQuery.Select<T> {
    query: lf.query.Select;
    resultMapping: (input: T) => T = input => input;

    constructor(protected db: lf.Database, protected table: Table<T>, ...columns: Column<T>[]) {
      super();
      this.query = db.select(...columns.map(column => column.column)).from(table.table);
    }

    innerJoin<JT>(table: string|DbSchema.TableDefinition<JT>|Table<JT>, column: Column<T>, targetColumn: Column<JT>,
                  where?: Predicate<JT>): SelectJoin<T> {
      ErrorUtil.checkparamNonNull(table, 'table');
      ErrorUtil.checkparamNonNull(column, 'column');
      ErrorUtil.checkparamNonNull(targetColumn, 'targetColumn');
      return new SelectJoin<T>(this.db, this.table, this.query).innerJoin(table, column, targetColumn, where);
    }

    leftOuterJoin<JT>(tableName: string|DbSchema.TableDefinition<JT>|Table<JT>, column: Column<T>,
                      targetColumn: Column<JT>, where?: Predicate<JT>): SelectJoin<T> {
      ErrorUtil.checkparamNonNull(tableName, 'tableName');
      ErrorUtil.checkparamNonNull(column, 'column');
      ErrorUtil.checkparamNonNull(targetColumn, 'targetColumn');
      return new SelectJoin<T>(this.db, this.table, this.query).leftOuterJoin(tableName, column, targetColumn, where);
    }

    exec(): Observable<T[]> {
      return super.exec().pipe(map((results: T[]) => results.map(this.resultMapping)));
    }
  }

  export class SelectJoin<T> extends BasicSelect<T> implements DbQuery.SelectJoin<T> {
    constructor(protected db: lf.Database, protected table: Table<any>, public query: lf.query.Select) {
      super();
    }

    innerJoin<JT>(tableDefinition: string|DbSchema.TableDefinition<JT>|Table<JT>, column: Column<T>, targetColumn: Column<JT>,
                  where?: Predicate<JT>): this {
      ErrorUtil.checkparamNonNull(tableDefinition, 'tableDefinition');
      ErrorUtil.checkparamNonNull(column, 'column');
      ErrorUtil.checkparamNonNull(targetColumn, 'targetColumn');
      const tableName: string = tableDefinition instanceof Table
        ? tableDefinition.name() : tableDefinition instanceof DbSchema.TableDefinition
          ? tableDefinition.name : tableDefinition;
      const table = this.db.getSchema().table(tableName);
      let predicate = column.column.eq(targetColumn.column);
      if (where) {
        predicate = lf.op.and(predicate, where);
      }
      this.query = this.query.innerJoin(table, predicate);
      return this;
    }

    leftOuterJoin<JT>(tableDefinition: string|DbSchema.TableDefinition<JT>|Table<JT>, column: Column<T>, targetColumn: Column<JT>,
                      where?: Predicate<JT>): this {
      ErrorUtil.checkparamNonNull(tableDefinition, 'tableDefinition');
      ErrorUtil.checkparamNonNull(column, 'column');
      ErrorUtil.checkparamNonNull(targetColumn, 'targetColumn');
      const tableName: string = tableDefinition instanceof Table
        ? tableDefinition.name() : tableDefinition instanceof DbSchema.TableDefinition
        ? tableDefinition.name : tableDefinition;
      const table = this.db.getSchema().table(tableName);
      let predicate = column.column.eq(targetColumn.column);
      if (where) {
        predicate = lf.op.and(predicate, where);
      }
      this.query = this.query.leftOuterJoin(table, predicate);
      return this;
    }

    exec(): Observable<{ [tableName: string]: any[] }[]> {
      return super.exec().pipe(map((values: any[]) => {
        values.map(value => {
          Object.keys(value).forEach(key => {
            if (key === this.table.name()) {
              value[key] = this.resultMapping(value[key]);
            } else {
              value[key] = Mapper.mapFromDatabase(value[key], {});
            }
          });
          return value;
        });
        return values;
      }));
    }
  }

  export class Insert<T> extends Builder implements DbQuery.Insert<T> {
    query: lf.query.Insert;

    constructor(db: lf.Database, protected table: lf.schema.Table) {
      super();
      this.query = db.insert().into(table);
    }

    values(rows: T[]): this {
      ErrorUtil.checkparamNonNull(rows, 'rows');
      this.query = this.query.values(rows.map(row => this.table.createRow(Mapper.mapToDatabase(row))));
      return this;
    }

    exec(): Observable<T[]> {
      return super.exec();
    }
  }

  export class InsertOrReplace<T> extends Insert<T> implements DbQuery.Insert<T> {
    query: lf.query.Insert;

    constructor(db: lf.Database, protected table: lf.schema.Table) {
      super(db, table);
      this.query = db.insertOrReplace().into(table);
    }
  }

  export class Update<T> extends Builder implements DbQuery.Update<T> {
    query: lf.query.Update;

    constructor(db: lf.Database, protected table: lf.schema.Table) {
      super();
      this.query = db.update(table);
    }

    set(column: (keyof T & string), value: any): this {
      ErrorUtil.checkparamNonNull(column, 'column');
      this.query = this.query.set(this.table[column], Mapper.mapToDatabase(value));
      return this;
    }

    where(predicate: Predicate<T>): this {
      ErrorUtil.checkparamNonNull(predicate, 'predicate');
      this.query = this.query.where(predicate.get());
      return this;
    }

    exec(): Observable<T[]> {
      return super.exec();
    }
  }

  export class Delete<T> extends Builder implements DbQuery.Delete<T> {
    query: lf.query.Delete;

    constructor(db: lf.Database, protected table: lf.schema.Table) {
      super();
      this.query = db.delete().from(table);
    }

    where(predicate: Predicate<T>): this {
      this.query = this.query.where(predicate.get());
      return this;
    }

    exec(): Observable<void> {
      return super.exec();
    }
  }

  export class Transaction implements DbQuery.Transaction {
    constructor(private transaction: lf.Transaction) {
    }

    exec(queries: LovefieldAccessorQuery.Builder[]): Observable<any> {
      ErrorUtil.checkparamNonNull(queries, 'queries');
      if (environment.showSql) {
        queries.forEach(query => {
          log.debug(query.query.toSql());
        });
      }
      return from(this.transaction.exec(queries.map(query => query.query)));
    }
  }
}

export enum StorageType {
  INDEXEDDB,
  SQLITE
}

export class LovefieldConnectedDatabase implements ConnectedDatabase {
  static instance: LovefieldConnectedDatabase = null;

  public constructor(public db: lf.Database) {}

  table<T = any>(tableName: string): LovefieldAccessorQuery.Table<T> {
    return new LovefieldAccessorQuery.Table(this.db, tableName);
  }

  transaction(type: DbQuery.TransactionType): DbQuery.Transaction {
    const lfType: lf.TransactionType
      = (type === DbQuery.TransactionType.READ_ONLY) ? lf.TransactionType.READ_ONLY : lf.TransactionType.READ_WRITE;
    return new LovefieldAccessorQuery.Transaction(this.db.createTransaction(lfType));
  }

  close(): Observable<any> {
    this.db.close();
    return of(null);
  }

  and(...predicates: LovefieldAccessorQuery.Predicate<any>[]): DbQuery.Predicate<any> {
    return new LovefieldAccessorQuery.Predicate(lf.op.and(...predicates.map(predicate => predicate.get())));
  }

  not(operand: LovefieldAccessorQuery.Predicate<any>): DbQuery.Predicate<any> {
    return new LovefieldAccessorQuery.Predicate(lf.op.not(operand.get()));
  }

  or(...predicates: LovefieldAccessorQuery.Predicate<any>[]): DbQuery.Predicate<any> {
    return new LovefieldAccessorQuery.Predicate(lf.op.or(...predicates.map(predicate => predicate.get())));
  }

  import(data: object): Observable<void> {
    return from(this.db.import(data));
  }

  export(): Observable<object> {
    return from(this.db.export());
  }
}


@Injectable()
export class LovefieldDbAccessor extends AbstractIndexedDbAccessor {
  public static readonly SCHEMA_NAME = 'wasteapp';

  public isInitiated = false;

  private readySubject: ReplaySubject<LovefieldConnectedDatabase> = new ReplaySubject<LovefieldConnectedDatabase>(1);
  private schemaBuilder: lf.schema.Builder;
  private storageType: StorageType;

  constructor(private deviceService: DeviceService) {
    super();
  }

  protected static mapLovefieldType(type: DbSchema.ColumnType): lf.Type {
    let result: lf.Type = null;
    switch (type) {
      case DbSchema.ColumnType.STRING:
      case DbSchema.ColumnType.BYTES:
        result = lf.Type.STRING;
        break;
      case DbSchema.ColumnType.INTEGER:
        result = lf.Type.INTEGER;
        break;
      case DbSchema.ColumnType.NUMBER:
        result = lf.Type.NUMBER;
        break;
      case DbSchema.ColumnType.BOOLEAN:
        result = lf.Type.BOOLEAN;
        break;
      case DbSchema.ColumnType.ARRAY:
        result = lf.Type.OBJECT;
        break;
      case DbSchema.ColumnType.DATE_TIME:
        result = lf.Type.DATE_TIME;
        break;
      case DbSchema.ColumnType.OBJECT:
        result = lf.Type.OBJECT;
        break;
    }
    return result;
  }

  protected static mapLovefieldConstraintAction(constraint: DbSchema.ConstraintAction): lf.ConstraintAction {
    let result: lf.ConstraintAction = null;
    switch (constraint) {
      case DbSchema.ConstraintAction.RESTRICT:
        result = lf.ConstraintAction.RESTRICT;
        break;
      case DbSchema.ConstraintAction.CASCADE:
        result = lf.ConstraintAction.CASCADE;
        break;
    }
    return result;
  }

  protected static getSchemaBuilder(): lf.schema.Builder {
    return lf.schema.create(LovefieldDbAccessor.SCHEMA_NAME, AbstractIndexedDbAccessor.getVersion());
  }

  protected static preparePrimaryKey(tableBuilder: TableBuilder, column: DbSchema.PrimaryKey): TableBuilder {
    return tableBuilder
      .addColumn(column.name, LovefieldDbAccessor.mapLovefieldType(column.type))
      .addPrimaryKey([column.name], column.autoIncrement);
  }

  protected static prepareColumn(tableBuilder: TableBuilder, column: DbSchema.Column): TableBuilder {
    tableBuilder = tableBuilder
      .addColumn(column.name, LovefieldDbAccessor.mapLovefieldType(column.type));
    if (column.unique) {
      tableBuilder = tableBuilder.addUnique(column.name + '_unique', [column.name]);
    }
    if (column.foreignKey) {
      tableBuilder = tableBuilder.addForeignKey(column.foreignKey.name, {
        local: column.name,
        ref: column.foreignKey.targetTableName + '.' + column.foreignKey.targetColumnName,
        action: LovefieldDbAccessor.mapLovefieldConstraintAction(column.foreignKey.action)
      });
    }
    return tableBuilder;
  }

  init(dbSchema: DbSchema.Schema): Observable<LovefieldConnectedDatabase> {
    this.checkDatabaseVersion();
    return this.checkDevice()
      .pipe(
        mergeMap(() => {
          try {
            this.schemaBuilder = LovefieldDbAccessor.getSchemaBuilder();
            return this.prepareSchema(dbSchema);
          } catch (error) {
            return throwError(error);
          }
        }),
        mergeMap(() => this.connect()),
        tap(connectedDb => this.databaseIsReady(connectedDb)),
        catchError(error => {
          this.readySubject.error(error);
          return throwError(error);
        })
      );
  }

  deleteDB(): Observable<any> {
    log.info('Suppression de la base de donnée locale');
    return new Observable(observer => {
      log.debug('deleting indexedDB ' + LovefieldDbAccessor.SCHEMA_NAME);
      const deleteRequest = window.indexedDB.deleteDatabase(LovefieldDbAccessor.SCHEMA_NAME);

      deleteRequest.onerror = (error) => {
        console.error('Erreur lors de la suppression de la base');
        observer.error(error);
      };

      deleteRequest.onsuccess = () => {
        log.debug('indexedDB ' + LovefieldDbAccessor.SCHEMA_NAME + ' deleted');
        observer.next();
        observer.complete();
      };
    });
  }

  ready(): Observable<LovefieldConnectedDatabase> {
    return this.readySubject;
  }

  private checkDevice(): Observable<any> {
    return this.deviceService.isAppRunningOnADevice()
      .pipe(
        tap((isRunningOnDevice) => {
          this.storageType = StorageType.INDEXEDDB;
          if (isRunningOnDevice && (window as any).sqlitePlugin && (window as any).sqlitePlugin.openDatabase) {
            // this.storageType = StorageType.SQLITE;
            (window as any).openDatabase = (name: string, version: string, description: string, size: number, callback: Function): any => {
              return (window as any).sqlitePlugin.openDatabase({
                name: name,
                location: 'default',
              }, callback);
            };
          }
        })
      );
  }

  private connect(): Observable<LovefieldConnectedDatabase> {
    log.debug('connect to db ' + LovefieldDbAccessor.SCHEMA_NAME);
    return from(this.schemaBuilder.connect({
      storeType: this.getStoreType()
    })).pipe(
      map((lovefieldDb: lf.Database) => new LovefieldConnectedDatabase(lovefieldDb))
    );
  }

  private getStoreType(): DataStoreType {
    return this.storageType === StorageType.SQLITE ? DataStoreType.WEB_SQL : DataStoreType.INDEXED_DB;
  }

  private databaseIsReady(connectedDb: LovefieldConnectedDatabase): void {
    log.info('database ready');
    this.isInitiated = true;
    this.readySubject.next(connectedDb);
  }

  private prepareSchema(dbSchema: DbSchema.Schema): Observable<any> {
    for (const table of dbSchema.tables) {
      const nullableColumns: string[] = [];
      let tableBuilder = this.schemaBuilder.createTable(table.name);
      if (table.primaryKey) {
        tableBuilder = LovefieldDbAccessor.preparePrimaryKey(tableBuilder, table.primaryKey);
      }
      for (const column of table.columns) {
        tableBuilder = LovefieldDbAccessor.prepareColumn(tableBuilder, column);
        if (column.nullable) {
          nullableColumns.push(column.name);
        }
      }
      tableBuilder = tableBuilder.addNullable(nullableColumns);
    }
    return of(null);
  }
}
