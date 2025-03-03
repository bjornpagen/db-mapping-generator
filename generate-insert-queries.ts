import fs from "node:fs/promises"
import path from "node:path"
import { drizzle } from "drizzle-orm/mysql2"
import mysql from "mysql2/promise"
import {
	mysqlSchema, // Added for schema support
	varchar,
	text,
	int,
	boolean,
	decimal,
	char,
	datetime
} from "drizzle-orm/mysql-core"
import type { Mapping, ColumnMapping } from "./mapping"
import type {
	Database,
	Table,
	Column,
	ForeignKeyConstraint
} from "./relational"
import type { MySqlColumnBuilder } from "drizzle-orm/mysql-core/columns/common"
import type { MySqlTable } from "drizzle-orm/mysql-core/table"
import { Errors } from "./errors"

// Hardcoded DB URL for testing
const DB_URL = "mysql://root:@localhost:3306"

// Helper function to create a Drizzle column from a Column type
function createDrizzleColumn(column: Column): MySqlColumnBuilder {
	const { name, typeInfo } = column
	switch (typeInfo.kind) {
		case "varchar":
			return varchar(name, { length: typeInfo.length ?? undefined })
		case "nvarchar":
			return varchar(name, { length: typeInfo.length ?? undefined })
		case "char":
			return char(name, { length: typeInfo.length ?? undefined })
		case "text":
			return text(name)
		case "integer":
			return int(name)
		case "boolean":
			return boolean(name)
		case "datetime":
			return datetime(name)
		case "numeric":
		case "decimal":
			return decimal(name, {
				precision: typeInfo.precision ?? undefined,
				scale: typeInfo.scale ?? undefined
			})
		case "money":
			return decimal(name, { precision: 19, scale: 4 })
		case "bit":
			return boolean(name)
		default:
			console.warn(
				`Unsupported column type: ${typeInfo.kind}, using text as fallback`
			)
			return text(name)
	}
}

// Helper function to create a Drizzle table within a schema
function createDrizzleTable(
	schema: ReturnType<typeof mysqlSchema>,
	table: Table
): MySqlTable {
	const columns = table.columns.reduce(
		(acc, column) => {
			acc[column.name] = createDrizzleColumn(column)
			return acc
		},
		{} as Record<string, MySqlColumnBuilder>
	)
	return schema.table(table.name, columns)
}

// Connect to MySQL using mysql2 and initialize Drizzle
export async function createDbConnection(url: string) {
	const connection = await mysql.createConnection(url)
	return drizzle({ client: connection })
}

// Loads the Mapping object from a JSON file
async function loadMapping(filePath: string): Promise<Mapping> {
	const readResult = await Errors.try(
		fs.readFile(path.resolve(filePath), "utf-8")
	)
	if (readResult.error) {
		if (
			readResult.error instanceof Error &&
			"code" in readResult.error &&
			readResult.error.code === "ENOENT"
		) {
			throw new Error(`File not found: ${filePath}`)
		}
		throw Errors.wrap(readResult.error, "Failed to read mapping file")
	}

	try {
		const json = JSON.parse(readResult.data)
		const missingFields = []
		if (!json.type) {
			missingFields.push("type")
		}
		if (json.type !== "mapping") {
			missingFields.push("type (expected 'mapping')")
		}
		if (!json.in) {
			missingFields.push("in")
		}
		if (!json.out) {
			missingFields.push("out")
		}
		if (!json.columnMappings) {
			missingFields.push("columnMappings")
		}
		if (missingFields.length > 0) {
			throw new Error(
				`Invalid mapping structure: missing or invalid fields: ${missingFields.join(", ")}`
			)
		}
		return json as Mapping
	} catch (error) {
		if (error instanceof Error) {
			throw Errors.wrap(error, "Failed to parse mapping file")
		}
		throw new Error(String(error))
	}
}

// Groups column mappings by their target table (e.g., "mysql.party")
function groupMappingsByTargetTable(
	mapping: Mapping
): Map<string, ColumnMapping[]> {
	const map = new Map<string, ColumnMapping[]>()
	for (const cm of mapping.columnMappings) {
		if (cm.destinationSchema && cm.destinationTable) {
			const tableKey = `${cm.destinationSchema}.${cm.destinationTable}`
			if (!map.has(tableKey)) {
				map.set(tableKey, [])
			}
			const mappings = map.get(tableKey)
			if (mappings) {
				mappings.push(cm)
			}
		}
	}
	return map
}

// Builds a dependency graph and computes insertion order based on foreign key constraints
function getInsertionOrder(database: Database): string[] {
	if (database.dialect !== "mysql") {
		throw new Error(
			"Target database dialect must be 'mysql' for this implementation"
		)
	}

	const tableMap: Record<string, Table> = {}
	for (const table of database.tables) {
		tableMap[`${table.schema}.${table.name}`] = table
	}

	const dependencies: Record<string, Set<string>> = {}
	const allTables = new Set<string>()
	for (const table of database.tables) {
		const tableKey = `${table.schema}.${table.name}`
		allTables.add(tableKey)
		dependencies[tableKey] = new Set()
	}

	for (const table of database.tables) {
		const tableKey = `${table.schema}.${table.name}`
		const fkConstraints = table.constraints.filter(
			(c): c is ForeignKeyConstraint => c.constraintType === "foreignKey"
		)
		for (const fk of fkConstraints) {
			const referencedTableKey = `${fk.referencedSchema}.${fk.referencedTable}`
			if (tableMap[referencedTableKey] && referencedTableKey !== tableKey) {
				dependencies[tableKey].add(referencedTableKey)
			}
		}
	}

	const visited = new Set<string>()
	const tempMark = new Set<string>()
	const order: string[] = []

	function visit(node: string) {
		if (tempMark.has(node)) {
			throw new Error(`Circular dependency detected involving table: ${node}`)
		}
		if (!visited.has(node)) {
			tempMark.add(node)
			for (const dep of dependencies[node]) {
				visit(dep)
			}
			tempMark.delete(node)
			visited.add(node)
			order.push(node)
		}
	}

	for (const table of allTables) {
		if (!visited.has(table)) {
			visit(table)
		}
	}

	return order
}

// Generates Drizzle insert queries for inspection
export async function generateInsertQueries(
	mapping: Mapping,
	dbUrl?: string
): Promise<string> {
	const targetDb = mapping.out
	if (targetDb.dialect !== "mysql") {
		throw new Error("Target database must be MySQL.")
	}
	const db = await createDbConnection(dbUrl || DB_URL)
	const groupedMappings = groupMappingsByTargetTable(mapping)
	const insertionOrder = getInsertionOrder(targetDb)

	// Create schema objects for each unique schema
	const schemas: Record<string, ReturnType<typeof mysqlSchema>> = {}
	for (const table of targetDb.tables) {
		const schemaName = table.schema
		if (!schemas[schemaName]) {
			schemas[schemaName] = mysqlSchema(schemaName)
		}
	}

	// Create target tables using their respective schemas
	const targetTables: Record<string, MySqlTable> = {}
	for (const table of targetDb.tables) {
		const schemaName = table.schema
		const schema = schemas[schemaName]
		const tableKey = `${schemaName}.${table.name}`
		targetTables[tableKey] = createDrizzleTable(schema, table)
	}

	let report = "Drizzle Insert Queries Report\n\n"
	report += "Target tables in insertion order:\n\n"

	for (const [index, tableKey] of insertionOrder.entries()) {
		const mappings = groupedMappings.get(tableKey) || []
		if (mappings.length > 0) {
			const table = targetTables[tableKey]
			if (!table) {
				console.warn(`Table definition not found for ${tableKey}; skipping`)
				continue
			}

			report += `${index + 1}. ${tableKey}\n`
			report += "   Insert Query:\n"

			// Build the values object with placeholder data indicating source mappings
			const values: Record<string, string> = {}
			const columnMap = new Map<string, ColumnMapping>()
			for (const cm of mappings) {
				const columnKey = cm.destinationColumn
				if (columnMap.has(columnKey)) {
					console.warn(`Multiple mappings for column ${tableKey}.${columnKey}`)
				} else {
					columnMap.set(columnKey, cm)
				}
			}

			for (const [column, cm] of columnMap) {
				const source = `${cm.sourceSchema}.${cm.sourceTable}.${cm.sourceColumn}`
				values[column] = `<${source}>` // Placeholder indicating source
			}

			// Generate Drizzle insert query and get SQL
			const insertQuery = db.insert(table).values(values)
			const { sql: insertSql, params } = insertQuery.toSQL()
			report += `   - ${insertSql}\n`
			report += `     Parameters: ${JSON.stringify(params)}\n`
			report += "   Notes:\n"
			for (const [column, cm] of columnMap) {
				const source = `${cm.sourceSchema}.${cm.sourceTable}.${cm.sourceColumn}`
				report += `     - ${column}: from ${source}, description: "${cm.description}"\n`
			}
			report += "\n"
		}
	}

	await (db.$client as mysql.Connection).end()
	return report
}

// Main function to execute the process
async function main() {
	const mappingFilePath = process.argv[2]
	if (!mappingFilePath) {
		console.error(
			"Usage: bun run generate-insert-queries.ts <mapping-file-path>"
		)
		process.exit(1)
	}

	try {
		const mapping = await loadMapping(mappingFilePath)
		const report = await generateInsertQueries(mapping)
		console.log(report)
		// Optionally save to file
		await fs.writeFile("insert_queries_report.txt", report)
	} catch (error) {
		console.error(
			"Error:",
			error instanceof Error ? error.message : String(error)
		)
		process.exit(1)
	}
}

if (require.main === module) {
	main()
}
