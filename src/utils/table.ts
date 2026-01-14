import { primaryKeys } from "./primaryKeys.js";
import { TableRow } from "./tableRow.js";
import type { Svupa } from "./svupa.js";
import { Conditions, Condition } from "./condition.js";
import { TableStore } from "./tableStore.js";

type Prefilter = {
  value: string,
  key: string
};

export class Table<T extends Record<string, any>> {
  name: string;
  schema: string;
  primaryKeys: primaryKeys;
  __store: TableStore<T>;
  conditions: Conditions;
  svupa: Svupa;
  channel: any;
  rowCallback: Function;
  pessimisticBackup: PessimisticBackup<T>;
  optimisticUpdates: boolean;
  __prefilter: Prefilter | undefined;

  constructor(
    svupa: Svupa,
    name: string,
    keys: primaryKeys | Array<string> | string,
    schema: string = "public",
    optimistic: boolean = false,
    prefilter: Prefilter | undefined = undefined
  ) {
    this.svupa = svupa;
    this.name = name;
    this.schema = schema;
    this.optimisticUpdates = optimistic;
    this.__prefilter = prefilter;
    this.pessimisticBackup = new PessimisticBackup(this);
    this.primaryKeys =
      keys instanceof primaryKeys ? keys : new primaryKeys(keys);
    this.__store = new TableStore<T>(this);
    this.conditions = new Conditions();
    this.rowCallback = (row: TableRow<T>) => {
      return row;
    };
  }

  async init() {
    await this._addInitialRows();
    await this._subscribeToChannel();
    return this;
  }

  async _subscribeToChannel() {
    let channel_name = `realtime_${this.schema}_${this.name}`;
    this.channel = this.svupa.supabase
      .channel(channel_name)
      .on(
        "postgres_changes",
        { 
          event: "*",
          schema: this.schema,
          table: this.name,
          // see note below. Generally only for when filter is unchanging
          filter: (
            this.__prefilter
            ? `${this.__prefilter.key}=eq.${this.__prefilter.value}`
            : undefined
          )
        },
        /*l
                Filters are not applied for two reasons:
                    1.  There is no AND operator in Supabase Realtime, so only filtering for single conditions is possible.
                    2.  We need updates on rows that are not relevant anymore (according to the filter) to delete them from the store.
                        For example, if only rows with score < 5 should be displayed and an update causes the score to increase beyond 5 in the database, the row must be deleted from the store.
                */
        (payload) => {
          // get the new/deleted row and event type
          const { tableRow, event, timestamp } =
            this.__parseSubscriptionPayload(payload);

          // check if payload is relevant for this store, i.e. if it actually is subscribed to
          if (tableRow.checkConditions(this.conditions) === false) {
            // if it is not relevant anymore, delete it from the store
            this._deleteInternal(tableRow);
          } else if (event === "INSERT" || event === "UPDATE") {
            this._upsertInternal(tableRow, timestamp);
          } else if (event === "DELETE") {
            this._deleteInternal(tableRow);
          }
        }
      )
      .subscribe();
  }

  _createPessimisticRowBackup(row) {
    this.pessimisticBackup.backup(row);
  }

  _releasePessimisticRowBackup(row) {
    return this.pessimisticBackup.release(row);
  }

  _getPessimisticRowBackup(row) {
    return this.pessimisticBackup.get(row.id);
  }

  addCondition(condition: Condition) {
    this.conditions.add(condition);
  }

  filter(condition: Condition) {
    this.addCondition(condition);
    return this;
  }

  getRow(keys: Record<string, any>): TableRow<T> | undefined {
    return this.__store.getRow(keys);
  }

  getFirstRow(): TableRow<T> | undefined {
    return this.__store.getFirstRow();
  }

  callback(callback: Function) {
    this.rowCallback = callback;
    return this;
  }

  __parseSubscriptionPayload(payload: any): {
    tableRow: TableRow<T>;
    event: string;
    timestamp: Date;
  } {
    /**
     * Parses the subscription payload from a supabase event and returns the row and the event type
     *
     * @param payload - The payload received from a supabase subscription
     * @returns The row and the event type
     *
     */
    let row = payload.new;
    const event = payload.eventType;
    const timestamp = new Date(payload.commit_timestamp);
    if (event === "DELETE") {
      row = payload.old;
    }

    const tableRow = new TableRow(row, this.primaryKeys);

    return { tableRow, event, timestamp };
  }

  /**
   * Retrieves all rows from the table and adds them to the store.
   */
  async _addInitialRows() {
    const pageSize = 1000;

    let countQuery = this.svupa.supabase
      .from(this.name)
      .select("*", { count: "exact", head: true })
      
    if (this.__prefilter) {
      countQuery = countQuery.eq(
        this.__prefilter.key,
        this.__prefilter.value
      );
    }
    
    // 1. Count query (list-scoped)
    const { count, error: countError } = await countQuery;

    if (countError || !count) {
      if (countError) console.warn("error", countError);
      return;
    }

    // 2. Paginated fetch (same filter as realtime)
    let start = 0;

    while (start < count) {
      const end = start + pageSize - 1;

      let fetchQuery = this.svupa.supabase
        .from(this.name)
        .select("*");
        
      if (this.__prefilter) {
        fetchQuery = fetchQuery.eq(
          this.__prefilter.key,
          this.__prefilter.value
        );
      }
      
      fetchQuery = fetchQuery.range(start, end);
      fetchQuery = this.conditions.toQuery(fetchQuery);

      const { data, error } = await fetchQuery;

      if (error) {
        console.warn("error", error);
        return;
      }

      for (const row of data ?? []) {
        this._upsertInternal(row);
      }

      start += pageSize;
    }
  }

  async _addRowsFromRangeRequest(query, start: number, end: number) {
    let { data, error } = await query.range(start, end);
    if (error) {
      console.warn("error", error);
    }
    for (let row of data) {
      this._upsertInternal(row);
    }
  }

  subscribe(run: (value: TableRow<T>[]) => void) {
    // Svelte Store contract: Subscribe to changes in the table.
    return this.__store.subscribe(run);
  }

  // Handle CRUD operations

  // Supabase
  async insert(row: Record<string, any>): Promise<boolean> {
    return await this.svupa.supabase
      .from(this.name)
      .insert(row)
      .then(({ status }) => (status >= 200) && (status < 300));
  }

  async update(
    row: Partial<T> | TableRow<T>
  ): Promise<{ invokeTime: Date; status: boolean }> {
    let invokeTime = new Date();
    if (this.optimisticUpdates) {
      this._createPessimisticRowBackup(row);
      this._upsertInternal(row);
    }
    const success = await this.svupa.supabase
      .from(this.name)
      .upsert(row)
      .then(({ status }) => (status >= 200) && (status < 300));
    if (this.optimisticUpdates && success) {
      this._releasePessimisticRowBackup(row);
    } else {
      const revokedRow = this._getPessimisticRowBackup(row);
      this._upsertInternal(revokedRow);
    }

    return {
      invokeTime: invokeTime,
      status: success,
    };
  }

  async delete(row: TableRow<T> | Partial<T>): Promise<boolean> {
    if (this.optimisticUpdates) {
      this._createPessimisticRowBackup(row);
      this._deleteInternal(row);
    }
    const success = await this.svupa.supabase
      .from(this.name)
      .delete()
      .match(row)
      .then(({ status }) => (status >= 200) && (status < 300));
    if (this.optimisticUpdates) {
      if (success) {
        this._releasePessimisticRowBackup(row);
        return true;
      } else {
        const revokedRow = this._getPessimisticRowBackup(row);
        this._upsertInternal(revokedRow);
        return false;
      }
    } else {
      this._releasePessimisticRowBackup(row);

      return success;
    }
  }

  // internal

  _upsertInternal(row: Partial<T> | BackupRow<T> | TableRow<T>, timestamp?: Date) {
    const tableRow = row instanceof TableRow ? row : new TableRow(row, this.primaryKeys);
    if (timestamp && this.pessimisticBackup.has(tableRow.id)) {
        // if the row is still in pessimistic backup and the change is invoked by a regular subscription, ignore the change
        return;
      }
    this.__store.upsert(row);
  }

  _deleteInternal(row: Partial<T> | TableRow<T>) {
    this.__store.delete(row);
  }

  // Basic getters

  getName() {
    return this.name;
  }

  getSchema() {
    return this.schema;
  }

  getFullTableName() {
    return `${this.schema}.${this.name}`;
  }

  getPrimaryKeys() {
    return this.primaryKeys;
  }

  getNumberOfPrimaryKeys() {
    return this.primaryKeys.getNumberOfKeys();
  }

  getPrimaryKeysstring() {
    return this.primaryKeys.getKeysstring();
  }

  getPrimaryKeysArray() {
    return this.primaryKeys.getKeys();
  }
}

export type BackupRow<T> = {
  row: TableRow<T>;
  subscribers: number;
};

class PessimisticBackup<T> {
  data: Map<string, BackupRow<T>>;
  table: Table<T>;

  constructor(table: Table<T>) {
    this.table = table;
    this.data = new Map();
  }

  backup(row: TableRow<T>) {
    if (this.has(row.id)) {
      this.data.get(row.id).subscribers++;
      return;
    }
    const backupRow: BackupRow<T> = { row: structuredClone(row), subscribers: 1 };
    this.data.set(row.id, backupRow);
  }

  release(row: TableRow<T>) {
    if (!this.has(row.id)) {
      throw new Error("Row not found in backup");
    }
    let backupRow = this.data.get(row.id);
    backupRow.subscribers--;
    if (backupRow.subscribers === 0) {
      this.data.delete(row.id);
    }
    return backupRow.row;
  }

  has(id: string) {
    return this.data.has(id);
  }

  get(id: string) {
    return this.data.get(id);
  }

  subscribers(id: string) {
    return this.data.get(id).subscribers;
  }
}
