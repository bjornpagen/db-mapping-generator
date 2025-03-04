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

/**
 * Identifies all primary and foreign key columns in the output database.
 * @param mapping The mapping object containing input and output databases.
 * @returns An object with primaryKeys and foreignKeys mappings.
 */
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

/**
 * Groups column mappings by destination table and column, excluding PK and FK mappings.
 * @param mapping The mapping object.
 * @returns Grouped mappings by table and column.
 */
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

/**
 * Resolves mappings into single SQL expressions, including PK and FK placeholders.
 * @param grouped Grouped mappings from previous step.
 * @param mapping The mapping object.
 * @returns Resolved mappings with SQL expressions or placeholders.
 */
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

/**
 * Resolves duplicate mappings for non-key columns into a single SQL expression.
 * @param duplicates Array of duplicate column mappings.
 * @returns The resolved SQL expression.
 */
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

/**
 * Checks the mapping status of primary keys for all tables.
 * @param mapping The mapping object.
 * @param grouped Grouped mappings.
 * @returns Mapping status for each table's primary key.
 */
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

/**
 * Builds a dependency graph based on foreign key relationships.
 * @param mapping The mapping object.
 * @returns Dependency graph where each table maps to its referenced tables.
 */
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

/**
 * Performs a topological sort on the dependency graph and detects cycles.
 * @param graph The dependency graph.
 * @returns An object with the topological order and a flag indicating cycles.
 */
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

/**
 * Determines whether to skip an insert statement based on PRD Step 3 conditions.
 * @param columnsToInsert Columns included in the insert.
 * @param values Values included in the insert.
 * @param pkColumn The primary key column.
 * @param tableKey The table identifier (schema.table).
 * @param referencedTables Set of tables referenced by others.
 * @returns True if the insert should be skipped, false otherwise.
 */
function shouldSkipInsert(
	columnsToInsert: string[],
	values: string[],
	pkColumn: string,
	tableKey: string,
	referencedTables: Set<string>
): boolean {
	return (
		columnsToInsert.length === 1 &&
		columnsToInsert[0] === pkColumn &&
		!referencedTables.has(tableKey)
	)
}

/**
 * Generates insert statements for non-cyclic tables, implementing Step 3 and 4.
 * Skips PK-only inserts for unreferenced tables, preserves non-PK-only inserts.
 * @param resolvedMappings Resolved mappings with placeholders.
 * @param primaryKeyStatus Primary key status for each table.
 * @param topologicalOrder Topologically sorted table order.
 * @param referencedTables Set of referenced tables.
 * @returns Array of insert statements.
 */
function generateInsertStatements(
	resolvedMappings: ResolvedMappings,
	primaryKeyStatus: Record<string, { primaryKey: string; isMapped: boolean }>,
	topologicalOrder: string[],
	referencedTables: Set<string>
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

		// Include all additional columns (Step 4: Preserve non-PK-only inserts)
		for (const column in tableMappings) {
			if (column !== pkColumn) {
				columnsToInsert.push(column)
				values.push(tableMappings[column])
			}
		}

		// Step 3: Skip PK-only inserts for unreferenced tables or those inserting only NULLs beyond pk
		if (
			!shouldSkipInsert(
				columnsToInsert,
				values,
				pkColumn,
				tableKey,
				referencedTables
			)
		) {
			const insertSql = `INSERT INTO ${schema}.${table} (${columnsToInsert.join(", ")}) VALUES (${values.join(", ")});`
			inserts.push(insertSql)
		}
	}

	return inserts
}

/**
 * Identifies foreign keys to set to NULL during insert to break cycles.
 * @param table The table to analyze.
 * @param topologicalOrder The topological order of tables.
 * @returns Array of FK columns to set to NULL.
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
 * Generates insert statements for tables with cycles, implementing Step 3 and 4.
 * Skips PK-only inserts for unreferenced tables, preserves non-PK-only inserts and cycle handling.
 * @param resolvedMappings Resolved mappings with placeholders.
 * @param primaryKeyStatus Primary key status for each table.
 * @param tables Output tables from the mapping.
 * @param topologicalOrder Topological order of tables.
 * @param referencedTables Set of referenced tables.
 * @returns Object with inserts and columns needing updates.
 */
function generateInsertStatementsWithCycles(
	resolvedMappings: ResolvedMappings,
	primaryKeyStatus: Record<string, { primaryKey: string; isMapped: boolean }>,
	tables: Table[],
	topologicalOrder: string[],
	referencedTables: Set<string>
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

		// Include primary key
		if (tableMappings[pkInfo.primaryKey]) {
			columnsToInsert.push(pkInfo.primaryKey)
			values.push(tableMappings[pkInfo.primaryKey])
		}

		// Identify FKs to set to NULL for cycle breaking
		const fkToSetNull = getForeignKeysToSetNull(table, topologicalOrder)

		// Include all additional columns (Step 4: Preserve non-PK-only inserts)
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

		// Step 3: Skip PK-only inserts or those inserting only NULLs for unreferenced tables
		if (
			!shouldSkipInsert(
				columnsToInsert,
				values,
				pkInfo.primaryKey,
				tableKey,
				referencedTables
			)
		) {
			const insertSql = `INSERT INTO ${schema}.${tableName} (${columnsToInsert.join(", ")}) VALUES (${values.join(", ")});`
			inserts.push(insertSql)
		}
	}

	return { inserts, updatesNeeded }
}

/**
 * Generates update statements to set FKs to their correct values after inserts.
 * @param resolvedMappings Resolved mappings with placeholders.
 * @param primaryKeyStatus Primary key status for each table.
 * @param tables Output tables from the mapping.
 * @param updatesNeeded Record of columns needing updates.
 * @returns Array of update statements.
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

/**
 * Generates the final SQL script, incorporating all steps including 3 and 4.
 * @param mapping The mapping object.
 * @returns The complete SQL script as a string.
 */
async function generateFinalSql(mapping: Mapping): Promise<string> {
	const grouped = groupMappingsByDestination(mapping)

	const resolvedResult = await Errors.try(resolveMappings(grouped, mapping))
	if (resolvedResult.error) {
		throw Errors.wrap(resolvedResult.error, "Failed to resolve mappings")
	}
	const resolvedMappings = resolvedResult.data

	const primaryKeyStatus = getPrimaryKeyMappingStatus(mapping, grouped)
	const graph = buildDependencyGraph(mapping)

	// Determine referenced tables (from Step 1, already completed)
	const referencedTables = new Set<string>()
	for (const tableKey in graph) {
		for (const dependency of graph[tableKey]) {
			referencedTables.add(dependency)
		}
	}

	const { order: topologicalOrder, hasCycles } = topologicalSort(graph)
	let sqlStatements: string[] = []

	if (hasCycles) {
		const cycleResult = Errors.trySync(() =>
			generateInsertStatementsWithCycles(
				resolvedMappings,
				primaryKeyStatus,
				mapping.out.tables,
				topologicalOrder,
				referencedTables
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

		sqlStatements = [
			"SET FOREIGN_KEY_CHECKS = 0;",
			"START TRANSACTION;",
			...inserts,
			...updatesResult.data,
			"COMMIT;",
			"SET FOREIGN_KEY_CHECKS = 1;"
		]
	} else {
		const insertsResult = Errors.trySync(() =>
			generateInsertStatements(
				resolvedMappings,
				primaryKeyStatus,
				topologicalOrder,
				referencedTables
			)
		)
		if (insertsResult.error) {
			throw Errors.wrap(insertsResult.error, "Failed to generate inserts")
		}
		sqlStatements = insertsResult.data
	}

	return sqlStatements.join("\n")
}

/**
 * Loads a mapping from a JSON file.
 * @param filePath Path to the mapping file.
 * @returns The parsed mapping object.
 */
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

/**
 * Main execution function to generate and write the SQL script.
 */
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
