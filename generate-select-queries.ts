import fs from "node:fs/promises"
import path from "node:path"
import type { Mapping, ColumnMapping } from "./mapping"
import type { Table } from "./relational"
import { Errors } from "./errors"

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

// Validates that all source tables and columns in the mappings exist in the source database
function validateMappings(mapping: Mapping): boolean {
	const sourceDb = mapping.in
	const tableMap: { [key: string]: Table } = {}
	for (const table of sourceDb.tables) {
		const tableKey = `${table.schema}.${table.name}`
		tableMap[tableKey] = table
	}
	for (const cm of mapping.columnMappings) {
		const tableKey = `${cm.sourceSchema}.${cm.sourceTable}`
		const table = tableMap[tableKey]
		if (!table) {
			console.error(`Table ${tableKey} not found in source database`)
			return false
		}
		const column = table.columns.find((c) => c.name === cm.sourceColumn)
		if (!column) {
			console.error(`Column ${cm.sourceColumn} not found in table ${tableKey}`)
			return false
		}
	}
	return true
}

// Groups column mappings by their source table (e.g., "dbo.User")
function groupMappingsByTable(mapping: Mapping): Map<string, ColumnMapping[]> {
	const map = new Map<string, ColumnMapping[]>()
	for (const cm of mapping.columnMappings) {
		const tableKey = `${cm.sourceSchema}.${cm.sourceTable}`
		if (!map.has(tableKey)) {
			map.set(tableKey, [])
		}
		const mappingsArray = map.get(tableKey)
		if (mappingsArray) {
			mappingsArray.push(cm)
		}
	}
	return map
}

// Generates a T-SQL SELECT query for a given table and its mappings
function generateSelectQuery(
	tableKey: string,
	mappings: ColumnMapping[]
): string {
	const columns = mappings.map((cm) => cm.sourceColumn)
	// Remove duplicates to avoid selecting the same column multiple times
	const uniqueColumns = [...new Set(columns)]
	const selectColumns = uniqueColumns.join(", ")
	return `SELECT ${selectColumns} FROM ${tableKey}`
}

// Generates all T-SQL SELECT queries for the source tables in the mapping
function generateAllSelectQueries(mapping: Mapping): string[] {
	const groupedMappings = groupMappingsByTable(mapping)
	const queries: string[] = []
	for (const [tableKey, mappings] of groupedMappings) {
		const query = generateSelectQuery(tableKey, mappings)
		queries.push(query)
	}
	return queries
}

// Quick-and-dirty main function to execute the process
async function main() {
	const mappingFilePath = process.argv[2]
	if (!mappingFilePath) {
		console.error(
			"Usage: bun run generate-select-queries.ts <mapping-file-path>"
		)
		process.exit(1)
	}

	try {
		const mapping = await loadMapping(mappingFilePath)
		if (!validateMappings(mapping)) {
			console.error("Validation failed. Aborting.")
			process.exit(1)
		}
		const queries = generateAllSelectQueries(mapping)
		for (const query of queries) {
			console.log(query)
		}
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
