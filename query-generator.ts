import fs from "node:fs/promises"
import type { ColumnMapping, Mapping } from "./mapping"
import type {
	ForeignKeyConstraint,
	PrimaryKeyConstraint,
	Table
} from "./relational"
import { Errors } from "./errors"

// Step 1: Group mappings by destination table and then by source table
type GroupedMappings = Record<string, Record<string, ColumnMapping[]>>

/**
 * Groups column mappings by their destination table and then by source table.
 * @param mapping The Mapping object containing columnMappings to organize.
 * @returns A nested structure where outer keys are "destinationSchema.destinationTable",
 *          inner keys are "sourceSchema.sourceTable", and values are arrays of ColumnMapping objects.
 */
export function groupMappingsByDestination(mapping: Mapping): GroupedMappings {
	const grouped: GroupedMappings = {}

	for (const colMapping of mapping.columnMappings) {
		const destTableKey = `${colMapping.destinationSchema}.${colMapping.destinationTable}`
		const sourceTableKey = `${colMapping.sourceSchema}.${colMapping.sourceTable}`

		if (!grouped[destTableKey]) {
			grouped[destTableKey] = {}
		}

		if (!grouped[destTableKey][sourceTableKey]) {
			grouped[destTableKey][sourceTableKey] = []
		}

		grouped[destTableKey][sourceTableKey].push(colMapping)
	}

	return grouped
}

// Step 2: Resolve mappings into single SQL expressions per source table
type ResolvedMappings = Record<
	string, // destTableKey
	Record<
		string, // sourceTableKey
		Record<string, string> // destColumn to resolved expression
	>
>

/**
 * Resolves mappings for each source table to destination table, ensuring exactly one mapping to the primary key.
 * @param grouped The grouped mappings from Step 1.
 * @param mapping The Mapping object to access table constraints.
 * @returns A promise resolving to a structure with SQL expressions for each column per source table per destination table.
 */
async function resolveMappings(
	grouped: GroupedMappings,
	mapping: Mapping
): Promise<ResolvedMappings> {
	const resolved: ResolvedMappings = {}

	// Get primary keys for destination tables
	const primaryKeys: Record<string, string> = {}
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
		primaryKeys[tableKey] = pkConstraint.columns[0]
	}

	for (const destTableKey in grouped) {
		resolved[destTableKey] = {}
		const pkColumn = primaryKeys[destTableKey]
		if (!pkColumn) {
			throw new Error(`No primary key found for ${destTableKey}`)
		}

		for (const sourceTableKey in grouped[destTableKey]) {
			const mappings = grouped[destTableKey][sourceTableKey]
			// Group mappings by destination column
			const columnMappings: Record<string, ColumnMapping[]> = {}
			for (const cm of mappings) {
				if (!columnMappings[cm.destinationColumn]) {
					columnMappings[cm.destinationColumn] = []
				}
				columnMappings[cm.destinationColumn].push(cm)
			}

			// Check primary key mapping
			const pkMappings = columnMappings[pkColumn] || []
			if (pkMappings.length !== 1) {
				throw new Error(
					`Exactly one mapping required for primary key ${pkColumn} in ${destTableKey} from ${sourceTableKey}, but found ${pkMappings.length}`
				)
			}

			// Resolve mappings
			const resolvedForSource: Record<string, string> = {}
			for (const destColumn in columnMappings) {
				const cms = columnMappings[destColumn]
				if (cms.length === 1) {
					const cm = cms[0]
					resolvedForSource[destColumn] =
						`${cm.sourceSchema}.${cm.sourceTable}.${cm.sourceColumn}`
				} else {
					// Multiple mappings to non-primary key column
					const result = await Errors.try(resolveDuplicateMapping(cms))
					if (result.error) {
						throw result.error
					}
					resolvedForSource[destColumn] = result.data
				}
			}

			resolved[destTableKey][sourceTableKey] = resolvedForSource
		}
	}

	return resolved
}

/**
 * Mock function to simulate resolving duplicate mappings into a single SQL expression.
 * In a real scenario, this would call an AI service like OpenAI.
 * @param duplicates Array of ColumnMapping objects targeting the same destination column.
 * @returns A promise resolving to the combined SQL expression.
 */
async function resolveDuplicateMapping(
	duplicates: ColumnMapping[]
): Promise<string> {
	const destColumn = duplicates[0].destinationColumn
	if (destColumn === "validFor") {
		return `CONCAT(${duplicates[0].sourceSchema}.${duplicates[0].sourceTable}.${duplicates[0].sourceColumn}, ' to ', ${duplicates[1].sourceSchema}.${duplicates[1].sourceTable}.${duplicates[1].sourceColumn})`
	}
	if (destColumn === "name") {
		return `COALESCE(${duplicates[0].sourceSchema}.${duplicates[0].sourceTable}.${duplicates[0].sourceColumn}, ${duplicates[1].sourceSchema}.${duplicates[1].sourceTable}.${duplicates[1].sourceColumn})`
	}
	throw new Error(`No mock resolution defined for column ${destColumn}`)
}

// Step 3: Identify each table's primary key and check if it's mapped
/**
 * Identifies the primary key for each table in the output database and checks if it is mapped.
 * @param mapping The Mapping object containing the output database tables.
 * @param grouped The grouped mappings from Step 1.
 * @returns A record where each key is "schema.table" and the value contains the primary key column name and a boolean indicating if it is mapped.
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

// Step 4: Build a dependency graph based on foreign keys
/**
 * Builds a dependency graph based on foreign key constraints in the output database.
 * @param mapping The Mapping object containing the output database tables.
 * @returns A record where each key is "schema.table" and the value is an array of tables it depends on.
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

// Step 5: Sort the tables by dependencies and spot cycles
/**
 * Performs a topological sort on the dependency graph and detects cycles.
 * @param graph The dependency graph from Step 4.
 * @returns An object with the topological order and a boolean indicating if cycles exist.
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

// Step 6: Generate Insert Statements for Non-Cyclic Tables
/**
 * Generates MySQL INSERT statements for non-cyclic tables in topological order, one per source table.
 * @param resolvedMappings Resolved mappings from Step 2.
 * @param primaryKeyStatus Primary key status from Step 3.
 * @param topologicalOrder Topological order from Step 5.
 * @returns An array of MySQL INSERT statements.
 */
function generateInsertStatements(
	resolvedMappings: ResolvedMappings,
	primaryKeyStatus: Record<string, { primaryKey: string; isMapped: boolean }>,
	topologicalOrder: string[]
): string[] {
	const inserts: string[] = []

	for (const tableKey of topologicalOrder) {
		const [schema, table] = tableKey.split(".")
		const sourceTables = resolvedMappings[tableKey] || {}
		for (const sourceTableKey in sourceTables) {
			const mappings = sourceTables[sourceTableKey]
			const columnsToInsert: string[] = Object.keys(mappings)
			const selectExpressions: string[] = columnsToInsert.map(
				(col) => mappings[col]
			)

			if (columnsToInsert.length > 0) {
				const insertSql = `INSERT INTO ${schema}.${table} (${columnsToInsert.join(", ")}) SELECT ${selectExpressions.join(", ")} FROM ${sourceTableKey};`
				inserts.push(insertSql)
			}
		}
	}

	return inserts
}

// Step 7: Handle Cycles (Helper Functions)
/**
 * Identifies foreign keys to set to NULL during INSERT based on topological order.
 * @param table The table to analyze.
 * @param topologicalOrder The order of tables.
 * @returns Array of column names to set to NULL.
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
 * Generates INSERT statements for tables with cycles, setting some foreign keys to NULL.
 * @param resolvedMappings Resolved mappings from Step 2.
 * @param primaryKeyStatus Primary key status from Step 3.
 * @param tables Output tables from the Mapping.
 * @param topologicalOrder Topological order from Step 5.
 * @returns INSERT statements and columns needing updates.
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

		const sourceTables = resolvedMappings[tableKey] || {}
		const sourceTableKey = Object.keys(sourceTables)[0] // Assume first source table
		const tableMappings = sourceTableKey ? sourceTables[sourceTableKey] : {}
		const pkInfo = primaryKeyStatus[tableKey]

		if (!pkInfo) {
			throw new Error(`No primary key info for ${tableKey}`)
		}

		const columnsToInsert: string[] = []
		const values: string[] = []

		if (pkInfo.isMapped && pkInfo.primaryKey in tableMappings) {
			columnsToInsert.push(pkInfo.primaryKey)
			values.push(tableMappings[pkInfo.primaryKey])
		}

		const fkToSetNull = getForeignKeysToSetNull(table, topologicalOrder)

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
 * Generates UPDATE statements to fix foreign keys set to NULL during INSERT.
 * @param resolvedMappings Resolved mappings from Step 2.
 * @param primaryKeyStatus Primary key status from Step 3.
 * @param tables Output tables from the Mapping.
 * @param updatesNeeded Columns needing updates from Step 7.
 * @returns Array of MySQL UPDATE statements.
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
		if (!pkInfo.isMapped) {
			throw new Error(`Cannot update ${tableKey}: primary key not mapped`)
		}

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

// Step 8: Combine Everything into the Final SQL
/**
 * Combines all generated SQL statements into the final MySQL script.
 * - No cycles: Lists INSERT statements in topological order.
 * - With cycles: Wraps INSERTs and UPDATEs in a transaction with foreign key checks disabled.
 * @param mapping The Mapping object.
 * @returns The final SQL script as a single string.
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
	const { order: topologicalOrder, hasCycles } = topologicalSort(graph)

	let sqlStatements: string[] = []

	if (hasCycles) {
		const cycleResult = Errors.trySync(() =>
			generateInsertStatementsWithCycles(
				resolvedMappings,
				primaryKeyStatus,
				mapping.out.tables,
				topologicalOrder
			)
		)
		if (cycleResult.error) {
			throw Errors.wrap(
				cycleResult.error,
				"Failed to generate SQL statements for cyclic dependencies"
			)
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
			throw Errors.wrap(
				updatesResult.error,
				"Failed to generate update statements"
			)
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
				topologicalOrder
			)
		)
		if (insertsResult.error) {
			throw Errors.wrap(
				insertsResult.error,
				"Failed to generate insert statements"
			)
		}
		sqlStatements = insertsResult.data
	}

	return sqlStatements.join("\n")
}

// Function to load mapping from a JSON file
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

// Run main if this is the entry point
if (require.main === module) {
	main().catch(console.error)
}
