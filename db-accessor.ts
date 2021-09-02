import { Observable } from 'rxjs/internal/Observable';
import { Injectable } from '@angular/core';

export namespace DbSchema {
  export class TableDefinition<T> {
    constructor(public name: string) {}
  }

  export class Schema {
    tables: Table<any>[] = [];

    createTable<T = any>(name: string|TableDefinition<T>): Table<T> {
      const newTable = new Table<T>(name);
      this.tables.push(newTable);
      return newTable;
    }
  }

  export class Table<T> {
    name: string;
    primaryKey: PrimaryKey;
    columns: Column[] = [];

    constructor(name: string|TableDefinition<T>) {
      this.name = name instanceof TableDefinition ? name.name : name;
    }

    addPrimaryKey(columnName: (keyof T & string), type: DbSchema.ColumnType, autoIncrement = false): this {
      this.primaryKey = new PrimaryKey(columnName, type, autoIncrement);
      return this;
    }

    addColumn(name: (keyof T & string), type: ColumnType, nullable = true, unique = false): this {
      this.columns.push(new Column(name, type, nullable, unique));
      return this;
    }

    addForeignKey(columnName: (keyof T & string), type: ColumnType, foreignKey: ForeignKey, nullable = true): this {
      this.columns.push(new Column(columnName, type)
        .setForeignKey(foreignKey.name, foreignKey.targetTableName, foreignKey.targetColumnName, foreignKey.action));
      return this;
    }
  }

  export class BasicColumn {
    name: string;
    type: ColumnType;

    constructor(name: string, type: ColumnType) {
      this.name = name;
      this.type = type;
    }
  }

  export class Column extends BasicColumn {
    nullable ?= true;
    unique ?= false;
    foreignKey ?: ForeignKey;

    constructor(name: string, type: ColumnType, nullable = true, unique = false) {
      super(name, type);
      this.nullable = nullable;
      this.unique = unique;
    }

    setForeignKey(name: string, targetTableName: string, targetColumnName: string,
                  action = ConstraintAction.CASCADE): this {
      this.foreignKey = new ForeignKey(name, targetTableName, targetColumnName, action);
      return this;
    }
  }

  export class PrimaryKey extends BasicColumn {
    autoIncrement ?= false;
    hidden ?= false;

    constructor(name: string, type: ColumnType, autoIncrement = false, hidden = false) {
      super(name, type);
      this.autoIncrement = autoIncrement;
      this.hidden = hidden;
    }
  }

  export class ForeignKey {
    name: string;
    targetTableName: string;
    targetColumnName: string;
    action: ConstraintAction;

    constructor(name: string, targetTableName: string, targetColumnName: string, action: ConstraintAction) {
      this.name = name;
      this.targetTableName = targetTableName;
      this.targetColumnName = targetColumnName;
      this.action = action;
    }
  }

  export enum ColumnType {
    STRING,
    NUMBER,
    BOOLEAN,
    ARRAY,
    DATE_TIME,
    OBJECT,
    INTEGER,
    BYTES
  }

  export enum ConstraintAction {
    RESTRICT,
    CASCADE
  }
}

export namespace DbQuery {
  import TableDefinition = DbSchema.TableDefinition;

  export interface Builder {
    exec(): Observable<any>;
  }

  export class Predicate<T> {}

  export class JoinPredicate {}

  export type ValueLiteral = string|number|boolean|Date;

  export enum Order { ASC, DESC }

  export enum TransactionType { READ_WRITE, READ_ONLY }

  export interface Table<T> {
    name(): string;

    select(...columns: (keyof T & string)[]): DbQuery.Select<T>;

    insert(): DbQuery.Insert<T>;

    insertOrReplace(): DbQuery.Insert<T>;

    delete(): DbQuery.Delete<T>;

    update(): DbQuery.Update<T>;

    update(): DbQuery.Update<T>;

    column(columnName: (keyof T & string)): DbQuery.Column<T>;
  }

  export interface Column<T> {
    eq(operand: ValueLiteral): Predicate<T>;
    neq(operand: ValueLiteral, skipCheckNotNull?): Predicate<T>;
    lt(operand: ValueLiteral): Predicate<T>;
    lte(operand: ValueLiteral): Predicate<T>;
    gt(operand: ValueLiteral): Predicate<T>;
    gte(operand: ValueLiteral): Predicate<T>;
    contains(operand: string): Predicate<T>;
    startWith(operand: string): Predicate<T>;
    endWith(operand: string): Predicate<T>;
    between(from: ValueLiteral, to: ValueLiteral): Predicate<T>;
    match(operand: RegExp): Predicate<T>;
    in(values: ValueLiteral[]): Predicate<T>;
    isNull(): Predicate<T>;
    isNotNull(): Predicate<T>;
  }

  export interface Delete<T> extends Builder {
    where(predicate: Predicate<T>): this;
    exec(): Observable<any>;
  }

  export interface Insert<T> extends Builder {
    values(rows: T[]): this;
    exec(): Observable<any>;
  }

  export interface Select<T> extends Builder {
    innerJoin<JT>(table: string|TableDefinition<JT>|Table<JT>, column: Column<T>, targetColumn: Column<JT>,
                  where?: Predicate<JT>): SelectJoin<T>;
    leftOuterJoin<JT>(table: string|TableDefinition<JT>|Table<JT>, column: Column<T>, targetColumn: Column<JT>,
                      where?: Predicate<JT>): SelectJoin<T>;
    limit(numberOfRows: number): this;
    orderBy(column: Column<T>, order?: Order): this;
    skip(numberOfRows: number): this;
    where(predicate: Predicate<T>): this;
    mapFrom(object: T): this;
    exec(): Observable<T[]>;
  }

  export interface SelectJoin<T> {
    innerJoin<JT>(table: string|TableDefinition<JT>|Table<JT>, column: Column<T>, targetColumn: Column<JT>,
                  where?: Predicate<JT>): this;
    leftOuterJoin<JT>(table: string|TableDefinition<JT>|Table<JT>, column: Column<T>, targetColumn: Column<JT>,
                      where?: Predicate<JT>): this;
    limit(numberOfRows: number): this;
    orderBy(column: Column<T>, order?: Order): this;
    skip(numberOfRows: number): this;
    where(predicate: Predicate<any>): this;
    mapFrom(object: T): this;
    exec(): Observable<{ [tableName: string]: any }[]>;
  }

    export interface Update<T> extends Builder {
    set(column: (keyof T & string), value: any): this;
    where(predicate: Predicate<T>): this;
    exec(): Observable<any>;
  }

  export interface Transaction {
    exec(queries: DbQuery.Builder[]): Observable<any>;
  }

  export class DbError extends Error {
    constructor(public description: string) {
      super(description);
    }
  }
}

@Injectable()
export abstract class DbAccessor {
  abstract init(schema: DbSchema.Schema): Observable<ConnectedDatabase>;

  abstract ready(): Observable<ConnectedDatabase>;

  abstract deleteDB(): Observable<any>;
}


export interface ConnectedDatabase {
  table<T = any>(tableName: string|DbSchema.TableDefinition<T>): DbQuery.Table<T>;

  and(...args: DbQuery.Predicate<any>[]): DbQuery.Predicate<any>;

  or(...args: DbQuery.Predicate<any>[]): DbQuery.Predicate<any>;

  transaction(type?: DbQuery.TransactionType): DbQuery.Transaction;

  close(): Observable<any>;
}
