import fs from "node:fs/promises"
import path from "node:path"
import type { Mapping, ColumnMapping } from "./mapping"
import type {
	Database,
	Table,
	ForeignKeyConstraint,
	PrimaryKeyConstraint,
	Constraint
} from "./relational"
import { Errors } from "./errors"
import OpenAI from "openai"
import { z } from "zod"
import { zodResponseFormat } from "openai/helpers/zod"
import { retryWithExponentialBackoff } from "./rate-limiter"

if (!process.env.OPENAI_API_KEY) {
	console.error("Error: OPENAI_API_KEY environment variable is not set")
	process.exit(1)
}

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY })

// Helper function to quote MySQL identifiers
function quoteIdentifier(name: string): string {
	return `\`${name.replace(/`/g, "``")}\``
}

// Schema for SQL expression generation
const SQLExpressionSchema = z.object({
	expression: z
		.string()
		.describe(
			"The SQL expression that combines all the source columns into a single value"
		)
})

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

// Loads the Mapping object from a JSON file
async function loadMapping(filePath: string): Promise<Mapping> {
	console.log(`Loading mapping file from: ${filePath}`)

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

	console.log("Successfully read mapping file, parsing JSON...")

	const parseResult = Errors.trySync(() => JSON.parse(readResult.data))
	if (parseResult.error) {
		throw Errors.wrap(parseResult.error, "Failed to parse mapping file")
	}

	const json = parseResult.data
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

	console.log(
		`Mapping loaded successfully with ${json.columnMappings.length} column mappings`
	)
	console.log(`Source DB: ${json.in.dialect}, Target DB: ${json.out.dialect}`)

	return json as Mapping
}

// Type for mapping groups
type MappingGroup = {
	sourceSchema: string
	sourceTable: string
	mappings: ColumnMapping[]
}

// Groups mappings by target table and source table
function groupMappingsByTargetTable(
	mapping: Mapping
): Map<string, MappingGroup[]> {
	console.log(
		`Grouping ${mapping.columnMappings.length} mappings by target table...`
	)

	const map = new Map<string, MappingGroup[]>()
	for (const cm of mapping.columnMappings) {
		if (cm.destinationSchema && cm.destinationTable) {
			const tableKey = `${cm.destinationSchema}.${cm.destinationTable}`
			if (!map.has(tableKey)) {
				map.set(tableKey, [])
			}
			const groups = map.get(tableKey) ?? []
			let group = groups.find(
				(g) =>
					g.sourceSchema === cm.sourceSchema && g.sourceTable === cm.sourceTable
			)
			if (!group) {
				group = {
					sourceSchema: cm.sourceSchema,
					sourceTable: cm.sourceTable,
					mappings: []
				}
				groups.push(group)
			}
			group.mappings.push(cm)
		}
	}

	console.log(`Grouped mappings into ${map.size} target tables`)
	for (const [tableKey, groups] of map.entries()) {
		console.log(
			`  - ${tableKey}: ${groups.length} source tables, ${groups.reduce((acc, g) => acc + g.mappings.length, 0)} mappings`
		)
	}

	return map
}

// Helper function to get the single primary key column for a table
function getPrimaryKeyColumn(table: Table): string | undefined {
	const pkConstraint = table.constraints.find(
		(c: Constraint) => c.constraintType === "primaryKey"
	) as PrimaryKeyConstraint | undefined
	return pkConstraint?.columns.length === 1
		? pkConstraint.columns[0]
		: undefined
}

// Helper function to get foreign key columns for a table
function getForeignKeyColumns(table: Table): string[] {
	return table.constraints
		.filter((c): c is ForeignKeyConstraint => c.constraintType === "foreignKey")
		.map((fk) => fk.sourceColumn)
}

// Builds a dependency graph and computes insertion order, handling cycles
function getInsertionOrder(database: Database): {
	nonCycleTables: string[]
	cycleTables: string[]
} {
	console.log(
		`Computing insertion order for ${database.tables.length} tables...`
	)

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

	console.log("Building dependency graph...")
	for (const table of database.tables) {
		const tableKey = `${table.schema}.${table.name}`
		const fkConstraints = table.constraints.filter(
			(c): c is ForeignKeyConstraint => c.constraintType === "foreignKey"
		)
		if (fkConstraints.length > 0) {
			console.log(
				`  - ${tableKey} has ${fkConstraints.length} foreign key constraints`
			)
		}
		for (const fk of fkConstraints) {
			const referencedTableKey = `${fk.referencedSchema}.${fk.referencedTable}`
			if (tableMap[referencedTableKey] && referencedTableKey !== tableKey) {
				dependencies[tableKey].add(referencedTableKey)
			}
		}
	}

	const visited = new Set<string>()
	const tempMark = new Set<string>()
	const stack: string[] = []
	const cycleTables = new Set<string>()

	function visit(node: string, path: string[] = []) {
		if (tempMark.has(node)) {
			const cycleStartIndex = path.indexOf(node)
			for (let i = cycleStartIndex; i < path.length; i++) {
				cycleTables.add(path[i])
			}
			cycleTables.add(node)
			console.log(`Detected cycle involving: ${node}`)
			return
		}
		if (!visited.has(node)) {
			tempMark.add(node)
			path.push(node)
			for (const dep of dependencies[node]) {
				visit(dep, [...path])
			}
			tempMark.delete(node)
			visited.add(node)
			stack.push(node)
		}
	}

	for (const table of allTables) {
		if (!visited.has(table)) {
			visit(table)
		}
	}

	const nonCycleTables = stack.filter((t) => !cycleTables.has(t))
	console.log(
		`Identified ${nonCycleTables.length} tables without cycles and ${cycleTables.size} tables in cycles`
	)

	return { nonCycleTables, cycleTables: Array.from(cycleTables) }
}

// Generates insert and update SQL strings with named placeholders
export async function generateInsertQueries(mapping: Mapping): Promise<{
	nonCycleInserts: { tableKey: string; sourceKey: string; sql: string }[]
	cycleTransaction: {
		inserts: { tableKey: string; sourceKey: string; sql: string }[]
		updates: { tableKey: string; sourceKey: string; sql: string }[]
	}
	warnings: string[]
}> {
	console.log(
		`Starting insert query generation for mapping with ${mapping.columnMappings.length} column mappings`
	)

	const targetDb = mapping.out
	if (targetDb.dialect !== "mysql") {
		throw new Error("Target database must be MySQL.")
	}
	const groupedMappings = groupMappingsByTargetTable(mapping)
	const { nonCycleTables, cycleTables } = getInsertionOrder(targetDb)
	const cycleTablesSet = new Set(cycleTables)
	const warnings: string[] = []

	// Collect warnings for non-nullable cycle FKs
	for (const tableKey of cycleTables) {
		const tableDef = targetDb.tables.find(
			(t) => `${t.schema}.${t.name}` === tableKey
		)
		if (!tableDef) {
			continue
		}
		const cycleFKColumns = tableDef.columns.filter((col) => {
			const fk = tableDef.constraints.find(
				(c) => c.constraintType === "foreignKey" && c.sourceColumn === col.name
			) as ForeignKeyConstraint | undefined
			return (
				fk && cycleTablesSet.has(`${fk.referencedSchema}.${fk.referencedTable}`)
			)
		})
		for (const col of cycleFKColumns) {
			if (!col.isNullable) {
				const warning = `Non-nullable cycle FK ${tableKey}.${col.name} will be set to NULL initially and updated later. Ensure this is acceptable.`
				console.log(`WARNING: ${warning}`)
				warnings.push(warning)
			}
		}
	}

	// Non-cycle inserts
	const nonCycleInserts: {
		tableKey: string
		sourceKey: string
		sql: string
	}[] = []
	for (const tableKey of nonCycleTables) {
		console.log(`Processing non-cycle table: ${tableKey}`)

		const mappingGroups = groupedMappings.get(tableKey) || []
		if (mappingGroups.length === 0) {
			console.log(`  - No mappings found for ${tableKey}, skipping`)
			continue
		}

		const tableDef = targetDb.tables.find(
			(t) => `${t.schema}.${t.name}` === tableKey
		)
		if (!tableDef) {
			warnings.push(`No table definition found for ${tableKey}`)
			continue
		}

		const pkColumn = getPrimaryKeyColumn(tableDef)
		const fkColumns = getForeignKeyColumns(tableDef)

		for (const group of mappingGroups) {
			const sourceKey = `${group.sourceSchema}.${group.sourceTable}`
			console.log(`  - Processing mappings from ${sourceKey}`)

			const mappings = group.mappings
			if (mappings.length === 0) {
				continue
			}

			// Separate mappings
			const pkMapping = mappings.find((cm) => cm.destinationColumn === pkColumn)
			const fkMappings = mappings.filter((cm) =>
				fkColumns.includes(cm.destinationColumn)
			)
			const otherMappings = mappings.filter(
				(cm) =>
					cm.destinationColumn !== pkColumn &&
					!fkColumns.includes(cm.destinationColumn)
			)

			// Handle non-key columns for potential duplicates
			const mappingMap = new Map<string, ColumnMapping[]>()
			for (const cm of otherMappings) {
				if (!mappingMap.has(cm.destinationColumn)) {
					mappingMap.set(cm.destinationColumn, [cm])
				} else {
					const mappings = mappingMap.get(cm.destinationColumn)
					if (mappings) {
						mappings.push(cm)
					}
				}
			}

			const resolvedMappings: { mapping: ColumnMapping; value: string }[] = []
			for (const [destCol, cms] of mappingMap.entries()) {
				if (cms.length === 1) {
					resolvedMappings.push({
						mapping: cms[0],
						value: `:${cms[0].sourceSchema}.${cms[0].sourceTable}.${cms[0].sourceColumn}`
					})
				} else {
					const combineResult = await Errors.try(resolveDuplicateMapping(cms))
					if (combineResult.error || !combineResult.data) {
						warnings.push(
							`Failed to combine duplicate mappings for ${tableKey}.${destCol} from ${sourceKey}`
						)
						resolvedMappings.push({
							mapping: cms[0],
							value: `:${cms[0].sourceSchema}.${cms[0].sourceTable}.${cms[0].sourceColumn}`
						})
					} else {
						resolvedMappings.push({
							mapping: cms[0],
							value: combineResult.data
						})
					}
				}
			}

			// Include primary key if mapped
			if (pkMapping) {
				resolvedMappings.push({
					mapping: pkMapping,
					value: `:${pkMapping.sourceSchema}.${pkMapping.sourceTable}.${pkMapping.sourceColumn}`
				})
			}

			// Include foreign keys if mapped
			for (const fkMapping of fkMappings) {
				if (!cycleTablesSet.has(tableKey)) {
					resolvedMappings.push({
						mapping: fkMapping,
						value: `:${fkMapping.sourceSchema}.${fkMapping.sourceTable}.${fkMapping.sourceColumn}`
					})
				}
			}

			if (resolvedMappings.length === 0) {
				console.log(`  - No resolved mappings for ${sourceKey}, skipping`)
				continue
			}

			const columns = resolvedMappings.map((item) =>
				quoteIdentifier(item.mapping.destinationColumn)
			)
			const values = resolvedMappings.map((item) => item.value)
			const tableFullName = `${quoteIdentifier(tableDef.schema)}.${quoteIdentifier(tableDef.name)}`
			const sql = `INSERT INTO ${tableFullName} (${columns.join(", ")}) VALUES (${values.join(", ")})`
			console.log(`  - Generated SQL for ${tableKey} from ${sourceKey}: ${sql}`)
			nonCycleInserts.push({ tableKey, sourceKey, sql })
		}
	}

	// Cycle inserts and updates
	const cycleInserts: { tableKey: string; sourceKey: string; sql: string }[] =
		[]
	const cycleUpdates: { tableKey: string; sourceKey: string; sql: string }[] =
		[]
	for (const tableKey of cycleTables) {
		console.log(`Processing cycle table: ${tableKey}`)

		const mappingGroups = groupedMappings.get(tableKey) || []
		if (mappingGroups.length === 0) {
			console.log(`  - No mappings found for ${tableKey}, skipping`)
			continue
		}

		const tableDef = targetDb.tables.find(
			(t) => `${t.schema}.${t.name}` === tableKey
		)
		if (!tableDef) {
			warnings.push(`No table definition found for ${tableKey}`)
			continue
		}

		const pkColumn = getPrimaryKeyColumn(tableDef)
		const fkColumns = getForeignKeyColumns(tableDef)
		const cycleFKColumns = tableDef.columns
			.filter((col) => {
				const fk = tableDef.constraints.find(
					(c) =>
						c.constraintType === "foreignKey" && c.sourceColumn === col.name
				) as ForeignKeyConstraint | undefined
				return (
					fk &&
					cycleTablesSet.has(`${fk.referencedSchema}.${fk.referencedTable}`)
				)
			})
			.map((col) => col.name)

		for (const group of mappingGroups) {
			const sourceKey = `${group.sourceSchema}.${group.sourceTable}`
			console.log(`  - Processing mappings from ${sourceKey}`)

			const mappings = group.mappings
			if (mappings.length === 0) {
				continue
			}

			// Separate mappings
			const pkMapping = mappings.find((cm) => cm.destinationColumn === pkColumn)
			const fkMappings = mappings.filter((cm) =>
				fkColumns.includes(cm.destinationColumn)
			)
			const otherMappings = mappings.filter(
				(cm) =>
					cm.destinationColumn !== pkColumn &&
					!fkColumns.includes(cm.destinationColumn)
			)

			// Handle non-key columns for potential duplicates
			const mappingMap = new Map<string, ColumnMapping[]>()
			for (const cm of otherMappings) {
				if (!mappingMap.has(cm.destinationColumn)) {
					mappingMap.set(cm.destinationColumn, [cm])
				} else {
					const mappings = mappingMap.get(cm.destinationColumn)
					if (mappings) {
						mappings.push(cm)
					}
				}
			}

			const resolvedMappings: { mapping: ColumnMapping; value: string }[] = []
			for (const [destCol, cms] of mappingMap.entries()) {
				if (cms.length === 1) {
					resolvedMappings.push({
						mapping: cms[0],
						value: `:${cms[0].sourceSchema}.${cms[0].sourceTable}.${cms[0].sourceColumn}`
					})
				} else {
					const combineResult = await Errors.try(resolveDuplicateMapping(cms))
					if (combineResult.error || !combineResult.data) {
						warnings.push(
							`Failed to combine duplicate mappings for ${tableKey}.${destCol} from ${sourceKey}`
						)
						resolvedMappings.push({
							mapping: cms[0],
							value: `:${cms[0].sourceSchema}.${cms[0].sourceTable}.${cms[0].sourceColumn}`
						})
					} else {
						resolvedMappings.push({
							mapping: cms[0],
							value: combineResult.data
						})
					}
				}
			}

			// Include primary key if mapped
			if (pkMapping) {
				resolvedMappings.push({
					mapping: pkMapping,
					value: `:${pkMapping.sourceSchema}.${pkMapping.sourceTable}.${pkMapping.sourceColumn}`
				})
			}

			// Prepare insert with cycle FKs as NULL
			const insertMappings = resolvedMappings.filter(
				(item) => !cycleFKColumns.includes(item.mapping.destinationColumn)
			)
			const insertColumns = insertMappings.map((item) =>
				quoteIdentifier(item.mapping.destinationColumn)
			)
			const insertValues = insertMappings.map((item) => item.value)
			const allColumns = insertColumns.concat(
				cycleFKColumns.map(quoteIdentifier)
			)
			const allValues = insertValues.concat(cycleFKColumns.map(() => "NULL"))
			const tableFullName = `${quoteIdentifier(tableDef.schema)}.${quoteIdentifier(tableDef.name)}`
			const insertSql = `INSERT INTO ${tableFullName} (${allColumns.join(", ")}) VALUES (${allValues.join(", ")})`
			console.log(`  - Generated insert SQL for ${sourceKey}: ${insertSql}`)
			cycleInserts.push({ tableKey, sourceKey, sql: insertSql })

			// Generate updates for cycle FKs
			if (pkColumn && pkMapping) {
				const whereClause = `${quoteIdentifier(pkColumn)} = :${pkMapping.sourceSchema}.${pkMapping.sourceTable}.${pkMapping.sourceColumn}`
				for (const cycleFK of cycleFKColumns) {
					const fkMapping = fkMappings.find(
						(cm) => cm.destinationColumn === cycleFK
					)
					if (fkMapping) {
						const updateSql = `UPDATE ${tableFullName} SET ${quoteIdentifier(cycleFK)} = :${fkMapping.sourceSchema}.${fkMapping.sourceTable}.${fkMapping.sourceColumn} WHERE ${whereClause}`
						console.log(`  - Generated update SQL for ${cycleFK}: ${updateSql}`)
						cycleUpdates.push({ tableKey, sourceKey, sql: updateSql })
					}
				}
			} else if (!pkMapping && pkColumn) {
				warnings.push(
					`No primary key mapping for ${tableKey}.${pkColumn} from ${sourceKey}; updates skipped for cycle FKs`
				)
			}
		}
	}

	console.log(
		`Query generation complete: ${nonCycleInserts.length} non-cycle inserts, ${cycleInserts.length} cycle inserts, ${cycleUpdates.length} updates, ${warnings.length} warnings`
	)

	return {
		nonCycleInserts,
		cycleTransaction: { inserts: cycleInserts, updates: cycleUpdates },
		warnings
	}
}

// Builds a report string from the SQL strings
function buildReport(
	result: Awaited<ReturnType<typeof generateInsertQueries>>
): string {
	console.log(
		`Building report with ${result.nonCycleInserts.length} non-cycle inserts, ${result.cycleTransaction.inserts.length} cycle inserts, ${result.cycleTransaction.updates.length} updates`
	)

	let report = "Insert Queries Report\n\n"

	if (result.warnings.length > 0) {
		report += "Warnings:\n"
		for (const warning of result.warnings) {
			report += `  - ${warning}\n`
		}
		report += "\n"
	}

	report += "Non-cycle tables in insertion order:\n\n"
	for (const [
		index,
		{ tableKey, sourceKey, sql }
	] of result.nonCycleInserts.entries()) {
		report += `${index + 1}. ${tableKey} from ${sourceKey}\n`
		report += "   Insert Query:\n"
		report += `   - ${sql}\n\n`
	}

	if (
		result.cycleTransaction.inserts.length > 0 ||
		result.cycleTransaction.updates.length > 0
	) {
		report += "Cycle tables:\n\n"
		report += "SET FOREIGN_KEY_CHECKS = 0;\n"
		report += "START TRANSACTION;\n\n"
		for (const { tableKey, sourceKey, sql } of result.cycleTransaction
			.inserts) {
			report += `   - ${sql}  -- ${tableKey} from ${sourceKey}\n`
		}
		for (const { tableKey, sourceKey, sql } of result.cycleTransaction
			.updates) {
			report += `   - ${sql}  -- ${tableKey} from ${sourceKey}\n`
		}
		report += "COMMIT;\n"
		report += "SET FOREIGN_KEY_CHECKS = 1;\n\n"
	}

	report += "Notes:\n"
	report +=
		"- Placeholders like `:source_schema.source_table.source_column` should be replaced with actual values from the source database.\n"

	console.log(`Report generated with ${report.length} characters`)
	return report
}

// Main function to execute the process and log the report
async function main() {
	console.log("Starting insert query generation process")

	const mappingFilePath = process.argv[2]
	if (!mappingFilePath) {
		console.error(
			"Usage: bun run generate-insert-queries.ts <mapping-file-path>"
		)
		process.exit(1)
	}

	console.log(`Loading mapping from file: ${mappingFilePath}`)
	const mappingResult = await Errors.try(loadMapping(mappingFilePath))
	if (mappingResult.error) {
		console.error(
			"Error:",
			mappingResult.error instanceof Error
				? mappingResult.error.message
				: String(mappingResult.error)
		)
		process.exit(1)
	}
	console.log(
		`Successfully loaded mapping with ${mappingResult.data.columnMappings.length} column mappings`
	)

	console.log("Generating insert queries...")
	const queriesResult = await Errors.try(
		generateInsertQueries(mappingResult.data)
	)
	if (queriesResult.error) {
		console.error(
			"Error:",
			queriesResult.error instanceof Error
				? queriesResult.error.message
				: String(queriesResult.error)
		)
		process.exit(1)
	}
	console.log("Insert queries generated successfully")

	console.log("Building report...")
	const report = buildReport(queriesResult.data)
	console.log("Report built successfully")
	console.log(report)

	console.log("Writing report to file: insert_queries_report.txt")
	const writeResult = await Errors.try(
		fs.writeFile("insert_queries_report.txt", report)
	)
	if (writeResult.error) {
		console.error(
			"Error writing report file:",
			writeResult.error instanceof Error
				? writeResult.error.message
				: String(writeResult.error)
		)
		process.exit(1)
	}
	console.log("Report file written successfully")
}

if (require.main === module) {
	main()
}
