// Root database structure
export interface Database {
	type: "database";
	dialect: "tsql" | "mysql";
	tables: Table[];
}

// Table structure
export interface Table {
	type: "table";
	name: string;
	schema: string;
	comment: string;
	rawSql: string;
	columns: Column[];
	constraints: Constraint[];
	fileGroup?: string;
}

// Column structure
export interface Column {
	type: "column";
	name: string;
	dataType: string;
	typeInfo: ColumnTypeInfo;
	isNullable: boolean;
	defaultValue?: string;
}

// Type information for columns
export type ColumnTypeInfo =
	| { kind: "varchar"; length: number | null }
	| { kind: "nvarchar"; length: number | null }
	| { kind: "char"; length: number | null }
	| { kind: "numeric"; precision: number | null; scale: number | null }
	| { kind: "decimal"; precision: number | null; scale: number | null }
	| { kind: "datetime" }
	| { kind: "money" }
	| { kind: "bit" }
	| { kind: "text" }
	| { kind: "simple" }
	| { kind: "boolean" }
	| { kind: "integer" };

// Constraint union type
export type Constraint =
	| PrimaryKeyConstraint
	| ForeignKeyConstraint
	| DefaultConstraint
	| UniqueConstraint
	| CheckConstraint;

// Primary Key constraint
export interface PrimaryKeyConstraint {
	type: "constraint";
	constraintType: "primaryKey";
	name: string;
	columns: string[];
	rawSql: string;
	clustered?: boolean;
	fileGroup?: string;
}

// Foreign Key constraint
export interface ForeignKeyConstraint {
	type: "constraint";
	constraintType: "foreignKey";
	name: string;
	sourceTable: string;
	sourceSchema: string;
	sourceColumn: string;
	referencedTable: string;
	referencedSchema: string;
	referencedColumn: string;
	rawSql: string;
}

// Default constraint
export interface DefaultConstraint {
	type: "constraint";
	constraintType: "default";
	name: string;
	table: string;
	schema: string;
	column: string;
	value: string;
	rawSql: string;
}

// Unique constraint
export interface UniqueConstraint {
	type: "constraint";
	constraintType: "unique";
	name: string;
	table: string;
	schema: string;
	columns: string[];
	clustered?: boolean;
	rawSql: string;
}

// Check constraint
export interface CheckConstraint {
	type: "constraint";
	constraintType: "check";
	name: string;
	table: string;
	schema: string;
	expression: string;
	rawSql: string;
}
