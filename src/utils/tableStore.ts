import { TableRow } from "./tableRow.js";
import { mapStore, type MapStore } from "./mapStore.js";
import { Table, type BackupRow } from "./table.js";
import { derived, type Readable } from "svelte/store";

export class TableStore<T extends Record<string, any>> {
  __store: MapStore<TableRow<T>>;
  __table: Table<T>;
  __externalStore: Readable<TableRow<T>[]>;

  constructor(table: Table<T>) {
    this.__store = mapStore();
    this.__table = table;
    this.__externalStore = derived(this.__store, ($store) => {
      return [...$store.values()];
    });
  }

  __convertRow(row: TableRow<T> | BackupRow<T> | Partial<T>): TableRow<T> {
    if (row instanceof TableRow) return row;
    if (row.row) { // BackupRow (TODO: improve this type check)
      return row.row; // fight the powah
    }
    return new TableRow<T>(row as T, this.__table.primaryKeys);
  }

  upsert(row: TableRow<T> | BackupRow<T> | Partial<T>) {
    const tableRow = this.__convertRow(row);
    this.__store.upsert(tableRow.id, tableRow.data);
  }

  delete(row: TableRow<T> | Partial<T>) {
    const tableRow = this.__convertRow(row);
    this.__store.remove(tableRow.id);
  }

  getRow(id: string | Record<string, any>): TableRow<T> | undefined {
    if (typeof id === "string") return this.__store.get(id);
    return this.getRow(this.__table.primaryKeys.generateRowID(id));
  }

  getFirstRow(): TableRow<T> | undefined {
    return this.__store.getFirstRow();
  }

  subscribe(run: (value: TableRow<T>[]) => void) {
    return this.__externalStore.subscribe(run);
  }
}
