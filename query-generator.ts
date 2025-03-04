import fs from "node:fs/promises"
import OpenAI from "openai"
import { z } from "zod"
import type { ColumnMapping, Mapping } from "./mapping"
import type {
	ForeignKeyConstraint,
	PrimaryKeyConstraint,
	Table
} from "./relational"
import { Errors } from "./errors"
import { zodResponseFormat } from "openai/helpers/zod"
import { retryWithExponentialBackoff } from "./rate-limiter"

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY })

const SQLExpressionSchema = z.object({
	expression: z
		.string()
		.describe(
			"The SQL expression that combines all the source columns into a single value"
		)
})

// Type definitions from previous steps
type GroupedMappings = Record<string, Record<string, ColumnMapping[]>>
type ResolvedMappings = Record<string, Record<string, string>>

// Step 1: Identify All Primary and Foreign Keys
function identifyKeyColumns(mapping: Mapping): {
	primaryKeys: Record<string, string>
	foreignKeys: Record<string, string[]>
} {
	const primaryKeys: Record<string, string> = {}
	const foreignKeys: Record<string, string[]> = {}

	for (const table of mapping.out.tables) {
		const tableKey = `${table.schema}.${table.name}`

		const pkConstraint = table.constraints.find(
			(c): c is PrimaryKeyConstraint => c.constraintType === "primaryKey"
		)
		if (pkConstraint && pkConstraint.columns.length === 1) {
			primaryKeys[tableKey] = pkConstraint.columns[0]
		} else {
			throw new Error(`Table ${tableKey} must have a single-column primary key`)
		}

		foreignKeys[tableKey] = []
		for (const constraint of table.constraints) {
			if (constraint.constraintType === "foreignKey") {
				const fkConstraint = constraint as ForeignKeyConstraint
				foreignKeys[tableKey].push(fkConstraint.sourceColumn)
			}
		}
	}

	return { primaryKeys, foreignKeys }
}

// Step 2: Group mappings by destination table and column, excluding PK and FK mappings
export function groupMappingsByDestination(mapping: Mapping): GroupedMappings {
	const { primaryKeys, foreignKeys } = identifyKeyColumns(mapping)
	const grouped: GroupedMappings = {}

	for (const colMapping of mapping.columnMappings) {
		const tableKey = `${colMapping.destinationSchema}.${colMapping.destinationTable}`
		const columnName = colMapping.destinationColumn

		const isPrimaryKey = primaryKeys[tableKey] === columnName
		const isForeignKey = (foreignKeys[tableKey] || []).includes(columnName)
		if (isPrimaryKey || isForeignKey) {
			continue
		}

		if (!grouped[tableKey]) {
			grouped[tableKey] = {}
		}
		if (!grouped[tableKey][columnName]) {
			grouped[tableKey][columnName] = []
		}
		grouped[tableKey][columnName].push(colMapping)
	}

	return grouped
}

// Step 3 & 4: Resolve mappings into single SQL expressions, including PK and FK mapping with placeholders
async function resolveMappings(
	grouped: GroupedMappings,
	mapping: Mapping
): Promise<ResolvedMappings> {
	const resolved: ResolvedMappings = {}
	const { primaryKeys } = identifyKeyColumns(mapping)
	const pkPlaceholders = new Map<string, string>()
	let placeholderCounter = 0

	for (const tableKey in grouped) {
		resolved[tableKey] = {}
		for (const column in grouped[tableKey]) {
			const mappings = grouped[tableKey][column]
			if (mappings.length === 1) {
				const cm = mappings[0]
				resolved[tableKey][column] =
					`:${cm.sourceSchema}.${cm.sourceTable}.${cm.sourceColumn}`
			} else {
				const result = await Errors.try(resolveDuplicateMapping(mappings))
				if (result.error) {
					throw result.error
				}
				resolved[tableKey][column] = result.data
			}
		}
	}

	for (const tableKey in primaryKeys) {
		if (!resolved[tableKey]) {
			resolved[tableKey] = {}
		}
		const pkColumn = primaryKeys[tableKey]
		const placeholder = `:pk_placeholder_${placeholderCounter++}`
		pkPlaceholders.set(tableKey, placeholder)
		resolved[tableKey][pkColumn] = placeholder
	}

	for (const table of mapping.out.tables) {
		const tableKey = `${table.schema}.${table.name}`
		for (const constraint of table.constraints) {
			if (constraint.constraintType === "foreignKey") {
				const fkConstraint = constraint as ForeignKeyConstraint
				const fkColumn = fkConstraint.sourceColumn
				const refTableKey = `${fkConstraint.referencedSchema}.${fkConstraint.referencedTable}`
				const refPlaceholder = pkPlaceholders.get(refTableKey)
				if (!refPlaceholder) {
					throw new Error(`No placeholder for referenced table ${refTableKey}`)
				}
				if (!resolved[tableKey]) {
					resolved[tableKey] = {}
				}
				resolved[tableKey][fkColumn] = refPlaceholder
			}
		}
	}

	return resolved
}

// Helper function to resolve duplicate mappings for non-key columns
async function resolveDuplicateMapping(
	duplicates: ColumnMapping[]
): Promise<string> {
	console.log(
		`Resolving duplicate mappings for ${duplicates[0].destinationColumn} with ${duplicates.length} source columns`
	)

	let prompt =
		"Combine the following column mapping descriptions into a single SQL expression that produces the merged value. Use named placeholders. For each mapping, consider the source column and its description.\n"
	duplicates.forEach((cm, index) => {
		prompt += `${index + 1}. Source: ${cm.sourceSchema}.${cm.sourceTable}.${cm.sourceColumn}. Description: ${cm.description}\n`
	})
	prompt +=
		"\nExample: For a mapping combining startDate and endDate into validFor, a valid output would be CONCAT(:dbo.Package.StartDate, ' to ', :dbo.Package.EndDate).\n"
	prompt += "Return only the SQL expression."

	console.log(
		`Sending prompt to OpenAI to resolve ${duplicates.length} duplicates for ${duplicates[0].destinationColumn}`
	)

	const result = await retryWithExponentialBackoff(async () => {
		const completionResult = await openai.beta.chat.completions.parse({
			model: "o3-mini",
			messages: [{ role: "user", content: prompt }],
			response_format: zodResponseFormat(SQLExpressionSchema, "sql")
		})

		const parsed = completionResult.choices[0].message.parsed
		if (!parsed) {
			throw new Error("OpenAI did not return a valid structured response")
		}

		return parsed.expression
	})

	console.log(`Successfully resolved duplicate mapping: ${result}`)
	return result
}

// Step 5: Identify each table's primary key and check if it's mapped
export function getPrimaryKeyMappingStatus(
	mapping: Mapping,
	grouped: GroupedMappings
): Record<string, { primaryKey: string; isMapped: boolean }> {
	const status: Record<string, { primaryKey: string; isMapped: boolean }> = {}

	for (const table of mapping.out.tables) {
		const tableKey = `${table.schema}.${table.name}`
		const pkConstraint = table.constraints.find(
			(c): c is PrimaryKeyConstraint => c.constraintType === "primaryKey"
		)
		if (!pkConstraint || pkConstraint.columns.length !== 1) {
			throw new Error(
				`Table ${tableKey} must have a single primary key constraint.`
			)
		}

		const primaryKey = pkConstraint.columns[0]
		const isMapped = !!grouped[tableKey]?.[primaryKey]?.length

		status[tableKey] = { primaryKey, isMapped }
	}

	return status
}

// Step 6: Build a dependency graph based on foreign keys
export function buildDependencyGraph(
	mapping: Mapping
): Record<string, string[]> {
	const graph: Record<string, string[]> = {}

	for (const table of mapping.out.tables) {
		const tableKey = `${table.schema}.${table.name}`
		graph[tableKey] = []

		for (const constraint of table.constraints) {
			if (constraint.constraintType === "foreignKey") {
				const fkConstraint = constraint as ForeignKeyConstraint
				const referencedKey = `${fkConstraint.referencedSchema}.${fkConstraint.referencedTable}`
				graph[tableKey].push(referencedKey)
			}
		}
	}

	return graph
}

// Step 7: Sort the tables by dependencies and spot cycles
export function topologicalSort(graph: Record<string, string[]>): {
	order: string[]
	hasCycles: boolean
} {
	const visiting = new Set<string>()
	const visited = new Set<string>()
	const order: string[] = []
	let hasCycles = false

	function dfs(node: string) {
		if (visited.has(node)) {
			return
		}
		if (visiting.has(node)) {
			hasCycles = true
			return
		}
		visiting.add(node)
		for (const dependency of graph[node] || []) {
			dfs(dependency)
		}
		visiting.delete(node)
		visited.add(node)
		order.push(node)
	}

	for (const node of Object.keys(graph)) {
		if (!visited.has(node)) {
			dfs(node)
		}
	}

	return { order: order.reverse(), hasCycles }
}

// Step 8: Generate Insert Statements for Non-Cyclic Tables
function generateInsertStatements(
	resolvedMappings: ResolvedMappings,
	primaryKeyStatus: Record<string, { primaryKey: string; isMapped: boolean }>,
	topologicalOrder: string[]
): string[] {
	const inserts: string[] = []

	for (const tableKey of topologicalOrder) {
		const [schema, table] = tableKey.split(".")
		const tableMappings = resolvedMappings[tableKey] || {}
		const pkInfo = primaryKeyStatus[tableKey]
		if (!pkInfo) {
			throw new Error(`No primary key info for table ${tableKey}`)
		}

		const columnsToInsert: string[] = []
		const values: string[] = []

		const pkColumn = pkInfo.primaryKey
		if (tableMappings[pkColumn]) {
			columnsToInsert.push(pkColumn)
			values.push(tableMappings[pkColumn])
		} else {
			throw new Error(
				`No mapping for primary key ${pkColumn} in table ${tableKey}`
			)
		}

		for (const column in tableMappings) {
			if (column !== pkColumn) {
				columnsToInsert.push(column)
				values.push(tableMappings[column])
			}
		}

		if (columnsToInsert.length > 0) {
			const insertSql = `INSERT INTO ${schema}.${table} (${columnsToInsert.join(", ")}) VALUES (${values.join(", ")});`
			inserts.push(insertSql)
		}
	}

	return inserts
}

// Step 7: Handle Cycles in Dependencies

/**
 * Identifies foreign keys to set to NULL during INSERT to break cycles, based on topological order.
 * If a referenced table appears after the current table in the order, its FK is set to NULL.
 * @param table The table to analyze for foreign key constraints.
 * @param topologicalOrder The order of tables from topological sort.
 * @returns Array of foreign key column names to set to NULL during insertion.
 */
function getForeignKeysToSetNull(
	table: Table,
	topologicalOrder: string[]
): string[] {
	const tableKey = `${table.schema}.${table.name}`
	const tableIndex = topologicalOrder.indexOf(tableKey)
	const fkToSetNull: string[] = []

	for (const constraint of table.constraints) {
		if (constraint.constraintType === "foreignKey") {
			const fkConstraint = constraint as ForeignKeyConstraint
			const referencedKey = `${fkConstraint.referencedSchema}.${fkConstraint.referencedTable}`
			const referencedIndex = topologicalOrder.indexOf(referencedKey)
			if (referencedIndex > tableIndex) {
				fkToSetNull.push(fkConstraint.sourceColumn)
			}
		}
	}

	return fkToSetNull
}

/**
 * Generates INSERT statements for tables, handling cycles by setting conflicting FKs to NULL.
 * Tracks which FKs need subsequent updates to their correct placeholder values.
 * @param resolvedMappings Mappings with placeholders for PKs and FKs from prior steps.
 * @param primaryKeyStatus Primary key status for each table.
 * @param tables The output tables from the Mapping object.
 * @param topologicalOrder The order of tables, possibly imperfect due to cycles.
 * @returns An object with INSERT statements and a record of columns needing updates.
 */
function generateInsertStatementsWithCycles(
	resolvedMappings: ResolvedMappings,
	primaryKeyStatus: Record<string, { primaryKey: string; isMapped: boolean }>,
	tables: Table[],
	topologicalOrder: string[]
): { inserts: string[]; updatesNeeded: Record<string, string[]> } {
	const inserts: string[] = []
	const updatesNeeded: Record<string, string[]> = {}

	for (const tableKey of topologicalOrder) {
		const [schema, tableName] = tableKey.split(".")
		const table = tables.find(
			(t) => t.schema === schema && t.name === tableName
		)
		if (!table) {
			throw new Error(`Table ${tableKey} not found`)
		}

		const tableMappings = resolvedMappings[tableKey] || {}
		const pkInfo = primaryKeyStatus[tableKey]
		if (!pkInfo) {
			throw new Error(`No primary key info for ${tableKey}`)
		}

		const columnsToInsert: string[] = []
		const values: string[] = []

		// Include primary key with its placeholder
		if (tableMappings[pkInfo.primaryKey]) {
			columnsToInsert.push(pkInfo.primaryKey)
			values.push(tableMappings[pkInfo.primaryKey])
		}

		// Identify FKs to set to NULL due to cycles
		const fkToSetNull = getForeignKeysToSetNull(table, topologicalOrder)

		// Process all mapped columns
		for (const column in tableMappings) {
			if (column === pkInfo.primaryKey) {
				continue
			}
			columnsToInsert.push(column)
			if (fkToSetNull.includes(column)) {
				values.push("NULL")
				updatesNeeded[tableKey] = updatesNeeded[tableKey] || []
				updatesNeeded[tableKey].push(column)
			} else {
				values.push(tableMappings[column])
			}
		}

		if (columnsToInsert.length > 0) {
			const insertSql = `INSERT INTO ${schema}.${tableName} (${columnsToInsert.join(", ")}) VALUES (${values.join(", ")});`
			inserts.push(insertSql)
		}
	}

	return { inserts, updatesNeeded }
}

/**
 * Generates UPDATE statements to set FKs (previously NULL) to their correct placeholder values after all inserts.
 * Uses the primary key placeholder to identify rows accurately.
 * @param resolvedMappings Mappings with placeholders for PKs and FKs.
 * @param primaryKeyStatus Primary key status for each table.
 * @param tables The output tables from the Mapping object.
 * @param updatesNeeded Record of tables and their FK columns needing updates.
 * @returns An array of MySQL UPDATE statements.
 */
function generateUpdateStatements(
	resolvedMappings: ResolvedMappings,
	primaryKeyStatus: Record<string, { primaryKey: string; isMapped: boolean }>,
	tables: Table[],
	updatesNeeded: Record<string, string[]>
): string[] {
	const updates: string[] = []

	for (const tableKey in updatesNeeded) {
		const [schema, tableName] = tableKey.split(".")
		const table = tables.find(
			(t) => t.schema === schema && t.name === tableName
		)
		if (!table) {
			throw new Error(`Table ${tableKey} not found`)
		}

		const columnsToUpdate = updatesNeeded[tableKey]
		if (!columnsToUpdate.length) {
			continue
		}

		const pkInfo = primaryKeyStatus[tableKey]
		const pkColumn = pkInfo.primaryKey
		const pkMapping = resolvedMappings[tableKey][pkColumn]
		if (!pkMapping) {
			throw new Error(`No mapping for primary key ${pkColumn} in ${tableKey}`)
		}

		const setClauses = columnsToUpdate.map((column) => {
			const mapping = resolvedMappings[tableKey][column]
			if (!mapping) {
				throw new Error(`No mapping for column ${column} in ${tableKey}`)
			}
			return `${column} = ${mapping}`
		})

		const updateSql = `UPDATE ${schema}.${tableName} SET ${setClauses.join(", ")} WHERE ${pkColumn} = ${pkMapping};`
		updates.push(updateSql)
	}

	return updates
}

// Step 8: Tie It All Together in generateFinalSql
async function generateFinalSql(mapping: Mapping): Promise<string> {
	// Step 2: Group mappings, excluding PKs and FKs
	const grouped = groupMappingsByDestination(mapping)

	// Steps 3 & 4: Resolve mappings with PKs and FKs using placeholders
	const resolvedResult = await Errors.try(resolveMappings(grouped, mapping))
	if (resolvedResult.error) {
		throw Errors.wrap(resolvedResult.error, "Failed to resolve mappings")
	}
	const resolvedMappings = resolvedResult.data

	// Step 5: Get primary key status for all tables
	const primaryKeyStatus = getPrimaryKeyMappingStatus(mapping, grouped)

	// Step 6: Build dependency graph based on FK relationships
	const graph = buildDependencyGraph(mapping)

	// Step 7: Determine insertion order and detect cycles
	const { order: topologicalOrder, hasCycles } = topologicalSort(graph)

	let sqlStatements: string[] = []

	if (hasCycles) {
		// Handle cycles (Step 7 continuation)
		const cycleResult = Errors.trySync(() =>
			generateInsertStatementsWithCycles(
				resolvedMappings,
				primaryKeyStatus,
				mapping.out.tables,
				topologicalOrder
			)
		)
		if (cycleResult.error) {
			throw Errors.wrap(cycleResult.error, "Failed to handle cycles")
		}
		const { inserts, updatesNeeded } = cycleResult.data

		const updatesResult = Errors.trySync(() =>
			generateUpdateStatements(
				resolvedMappings,
				primaryKeyStatus,
				mapping.out.tables,
				updatesNeeded
			)
		)
		if (updatesResult.error) {
			throw Errors.wrap(updatesResult.error, "Failed to generate updates")
		}

		// Wrap in transaction and disable FK checks for cycles
		sqlStatements = [
			"SET FOREIGN_KEY_CHECKS = 0;",
			"START TRANSACTION;",
			...inserts,
			...updatesResult.data,
			"COMMIT;",
			"SET FOREIGN_KEY_CHECKS = 1;"
		]
	} else {
		// No cycles, generate inserts in topological order
		const insertsResult = Errors.trySync(() =>
			generateInsertStatements(
				resolvedMappings,
				primaryKeyStatus,
				topologicalOrder
			)
		)
		if (insertsResult.error) {
			throw Errors.wrap(insertsResult.error, "Failed to generate inserts")
		}
		sqlStatements = insertsResult.data
	}

	// Return final SQL script as a single string
	return sqlStatements.join("\n")
}

// Load mapping from a JSON file
async function loadMapping(filePath: string): Promise<Mapping> {
	const fileResult = await Errors.try(fs.readFile(filePath, "utf-8"))
	if (fileResult.error) {
		throw Errors.wrap(
			fileResult.error,
			`Failed to load mapping from ${filePath}`
		)
	}

	const result = Errors.trySync(() => JSON.parse(fileResult.data) as Mapping)
	if (result.error) {
		throw Errors.wrap(
			result.error,
			`Failed to parse mapping JSON from ${filePath}`
		)
	}
	return result.data
}

// Main function to execute the process
async function main() {
	console.log("Starting insert query generation process")

	const mappingFilePath = process.argv[2]
	if (!mappingFilePath) {
		console.error("Usage: bun run query-generator.ts <mapping-file-path>")
		process.exit(1)
	}

	console.log(`Loading mapping from file: ${mappingFilePath}`)

	const mappingResult = await Errors.try(loadMapping(mappingFilePath))
	if (mappingResult.error) {
		console.error("Error:", mappingResult.error.toString())
		process.exit(1)
	}
	const mapping = mappingResult.data
	console.log(
		`Successfully loaded mapping with ${mapping.columnMappings.length} column mappings`
	)

	console.log("Generating insert queries...")

	const sqlResult = await Errors.try(generateFinalSql(mapping))
	if (sqlResult.error) {
		console.error("Error:", sqlResult.error.toString())
		process.exit(1)
	}
	const sqlScript = sqlResult.data
	console.log("Insert queries generated successfully")

	const outputFile = "insert_queries.sql"
	console.log(`Writing SQL script to file: ${outputFile}`)

	const writeResult = await Errors.try(fs.writeFile(outputFile, sqlScript))
	if (writeResult.error) {
		console.error("Error writing SQL file:", writeResult.error.toString())
		process.exit(1)
	}

	console.log("SQL script file written successfully")
}

if (require.main === module) {
	main().catch(console.error)
}
