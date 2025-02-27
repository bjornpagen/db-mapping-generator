import type { Database, Table } from "./relational";

export interface Mapping<DbKeys extends string = string> {
	type: "mapping";
	in: Record<DbKeys, Database>;
	out: Database;
	columnMappings: ColumnMapping<DbKeys>[];
}

export interface SourceColumn<DbKey extends string = string> {
	databaseId: DbKey;
	schema: string;
	table: string;
	column: string;
}

export interface ColumnMapping<DbKeys extends string = string> {
	sources: SourceColumn<DbKeys>[];
	destinationSchema: string;
	destinationTable: string;
	destinationColumn: string;
	description: string;
}

export function findTable<DbKeys extends string>(
	databases: Record<DbKeys, Database>,
	databaseId: DbKeys,
	schema: string,
	tableName: string,
): Table | undefined {
	const database = databases[databaseId];
	return database.tables.find(
		(table: Table) => table.schema === schema && table.name === tableName,
	);
}

export function createMapping<DbKeys extends string>(
	input: Record<DbKeys, Database>,
	output: Database,
	columnMappings: ColumnMapping<DbKeys>[],
): Mapping<DbKeys> {
	return {
		type: "mapping",
		in: input,
		out: output,
		columnMappings,
	};
}

export function createDirectColumnMapping<DbKeys extends string>(
	sources: SourceColumn<DbKeys>[],
	destSchema: string,
	destTable: string,
	destColumn: string,
	description: string,
): ColumnMapping<DbKeys> {
	return {
		sources,
		destinationSchema: destSchema,
		destinationTable: destTable,
		destinationColumn: destColumn,
		description,
	};
}

export function createSingleSourceColumnMapping<DbKeys extends string>(
	input: Record<DbKeys, Database>,
	databaseId: keyof typeof input & DbKeys,
	sourceSchema: string,
	sourceTable: string,
	sourceColumn: string,
	destSchema: string,
	destTable: string,
	destColumn: string,
	description: string,
): ColumnMapping<DbKeys> {
	return createDirectColumnMapping<DbKeys>(
		[
			{
				databaseId,
				schema: sourceSchema,
				table: sourceTable,
				column: sourceColumn,
			},
		],
		destSchema,
		destTable,
		destColumn,
		description,
	);
}

export function createTransformColumnMapping<DbKeys extends string>(
	input: Record<DbKeys, Database>,
	databaseId: keyof typeof input & DbKeys,
	sourceSchema: string,
	sourceTable: string,
	sourceColumn: string,
	destSchema: string,
	destTable: string,
	destColumn: string,
	description: string,
): ColumnMapping<DbKeys> {
	return createSingleSourceColumnMapping<DbKeys>(
		input,
		databaseId,
		sourceSchema,
		sourceTable,
		sourceColumn,
		destSchema,
		destTable,
		destColumn,
		description,
	);
}

export function addSourceToMapping<DbKeys extends string>(
	mapping: ColumnMapping<DbKeys>,
	input: Record<DbKeys, Database>,
	databaseId: keyof typeof input & DbKeys,
	schema: string,
	table: string,
	column: string,
): ColumnMapping<DbKeys> {
	return {
		...mapping,
		sources: [
			...mapping.sources,
			{
				databaseId,
				schema,
				table,
				column,
			},
		],
	};
}

export function createMultiSourceColumnMapping<DbKeys extends string>(
	sources: SourceColumn<DbKeys>[],
	destSchema: string,
	destTable: string,
	destColumn: string,
	description: string,
): ColumnMapping<DbKeys> {
	return {
		sources,
		destinationSchema: destSchema,
		destinationTable: destTable,
		destinationColumn: destColumn,
		description,
	};
}
