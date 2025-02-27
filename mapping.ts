import type { Database, Table } from "./relational"

// Mapping interface with a single input database
export interface Mapping {
	type: "mapping"
	in: Database
	out: Database
	columnMappings: ColumnMapping[]
}

// ColumnMapping interface with a single source
export interface ColumnMapping {
	sourceSchema: string
	sourceTable: string
	sourceColumn: string
	destinationSchema: string
	destinationTable: string
	destinationColumn: string
	description: string
}

// Find a table in the single input database
export function findTable(
	database: Database,
	schema: string,
	tableName: string
): Table | undefined {
	return database.tables.find(
		(table: Table) => table.schema === schema && table.name === tableName
	)
}

// Create a mapping with a single input database
export function createMapping(
	input: Database,
	output: Database,
	columnMappings: ColumnMapping[]
): Mapping {
	return {
		type: "mapping",
		in: input,
		out: output,
		columnMappings
	}
}

// Create a column mapping with a single source
export function createColumnMapping(
	sourceSchema: string,
	sourceTable: string,
	sourceColumn: string,
	destSchema: string,
	destTable: string,
	destColumn: string,
	description: string
): ColumnMapping {
	return {
		sourceSchema,
		sourceTable,
		sourceColumn,
		destinationSchema: destSchema,
		destinationTable: destTable,
		destinationColumn: destColumn,
		description
	}
}
