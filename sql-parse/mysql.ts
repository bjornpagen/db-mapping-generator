import * as fs from "node:fs"
import * as path from "node:path"
import { Parser } from "node-sql-parser"
import type {
	Database,
	Table,
	Column,
	ColumnTypeInfo,
	Constraint,
	ForeignKeyConstraint,
	PrimaryKeyConstraint
} from "../relational"

const parser = new Parser()

// Interfaces for AST nodes (aligned with node-sql-parser output)
interface ParseResult {
	ast: Create
}

interface Create {
	type: string
	keyword: string
	table?: Array<{ db?: string | null; table: string }>
	create_definitions?: CreateDefinition[]
}

interface CreateDefinition {
	resource: "column" | "constraint"
	column?: { column: string }
	definition?: {
		dataType: string
		length?: string | number
		suffix?: string[]
	}
	nullable?: { type: "not null" | "null"; value: string }
	primary_key?: string
	constraint_type?: string
	columns?: Array<{
		type: "column_ref"
		table: string | null
		column: string
	}>
	reference_definition?: {
		definition: Array<{
			type: "column_ref"
			table: string | null
			column: string
		}>
		table: Array<{ db: string | null; table: string; as: string | null }>
	}
}

function parseColumn(def: CreateDefinition): Column {
	const columnName = def.column?.column || "unknown"
	const dataTypeDef = def.definition || { dataType: "unknown" }
	let length: number | null = null
	if (dataTypeDef.length) {
		const lengthValue = dataTypeDef.length
		length =
			typeof lengthValue === "string"
				? Number.parseInt(lengthValue, 10)
				: lengthValue
	}

	// Map MySQL data type to our type system
	let typeInfo: ColumnTypeInfo
	const dataTypeStr = dataTypeDef.dataType.toLowerCase()

	if (dataTypeStr.includes("varchar")) {
		typeInfo = { kind: "varchar", length }
	} else if (dataTypeStr.includes("nvarchar")) {
		typeInfo = { kind: "nvarchar", length }
	} else if (dataTypeStr.includes("char")) {
		typeInfo = { kind: "char", length }
	} else if (dataTypeStr === "numeric" || dataTypeStr === "decimal") {
		// For simplicity, using length as precision and no scale
		typeInfo = {
			kind: dataTypeStr === "numeric" ? "numeric" : "decimal",
			precision: length,
			scale: null
		}
	} else if (dataTypeStr === "datetime") {
		typeInfo = { kind: "datetime" }
	} else if (dataTypeStr === "money") {
		typeInfo = { kind: "money" }
	} else if (dataTypeStr === "bit") {
		typeInfo = { kind: "bit" }
	} else if (dataTypeStr === "text") {
		typeInfo = { kind: "text" }
	} else if (dataTypeStr === "boolean" || dataTypeStr === "bool") {
		typeInfo = { kind: "boolean" }
	} else if (
		dataTypeStr === "int" ||
		dataTypeStr === "integer" ||
		dataTypeStr === "bigint"
	) {
		typeInfo = { kind: "integer" }
	} else {
		typeInfo = { kind: "simple" }
	}

	const isNullable = def.nullable?.type !== "not null"

	return {
		type: "column",
		name: columnName,
		dataType: dataTypeDef.dataType,
		typeInfo,
		isNullable,
		defaultValue: undefined // We're not parsing default values here
	}
}

function parseConstraint(
	def: CreateDefinition,
	table: string,
	schema: string
): Constraint | null {
	if (def.resource !== "constraint") {
		return null
	}

	const constraintType = def.constraint_type?.toLowerCase()

	if (constraintType === "foreign key") {
		if (!def.reference_definition) {
			console.warn("Foreign key constraint without reference_definition", def)
			return null
		}
		const sourceColumns = def.columns?.map((col) => col.column) || ["unknown"]
		const referencedTableInfo = def.reference_definition.table[0].table.split(
			"."
		) || ["unknown"]
		const referencedSchema =
			referencedTableInfo.length > 1 ? referencedTableInfo[0] : schema
		const referencedTable =
			referencedTableInfo.length > 1
				? referencedTableInfo[1]
				: referencedTableInfo[0]
		const referencedColumns = def.reference_definition.definition.map(
			(col) => col.column
		) || ["unknown"]
		// Assuming single-column foreign keys per the input SQL
		const sourceColumn = sourceColumns[0]
		const referencedColumn = referencedColumns[0]
		const name = "" // No name provided in the AST

		return {
			type: "constraint",
			constraintType: "foreignKey",
			name,
			sourceTable: table,
			sourceSchema: schema,
			sourceColumn,
			referencedTable,
			referencedSchema,
			referencedColumn,
			rawSql: ""
		} as ForeignKeyConstraint
	}

	// Add support for other constraint types if needed in the future
	return null
}

function processCreateTable(
	ast: Create,
	rawSql: string,
	comment: string,
	tables: { [key: string]: Table }
): void {
	const tableInfo = ast.table?.[0]
	if (!tableInfo || !tableInfo.table) {
		console.warn(`No table info found in CREATE TABLE: ${rawSql}`)
		console.log("Full AST:", JSON.stringify(ast, null, 2))
		return
	}
	const tableName = tableInfo.table.replace(/`/g, "")
	const schema = tableInfo.db?.replace(/`/g, "") || "mysql"
	const key = `${schema}.${tableName}`

	const definitions = ast.create_definitions || []
	const columns: Column[] = []
	let constraints: Constraint[] = []
	const primaryKeyColumns: string[] = []

	for (const def of definitions) {
		if (def.resource === "column") {
			const column = parseColumn(def)
			columns.push(column)
			if (def.primary_key) {
				primaryKeyColumns.push(column.name)
			}
		} else if (def.resource === "constraint") {
			const constraint = parseConstraint(def, tableName, schema)
			if (constraint) {
				constraints.push(constraint)
			}
		}
	}

	if (primaryKeyColumns.length > 0) {
		const primaryKeyConstraint: PrimaryKeyConstraint = {
			type: "constraint",
			constraintType: "primaryKey",
			name: "", // No name provided in the SQL
			columns: primaryKeyColumns,
			rawSql: ""
		}
		constraints.push(primaryKeyConstraint)
	}

	// Manually parse foreign key constraints from raw SQL
	const fkLines = rawSql
		.split("\n")
		.map((line) => line.trim())
		.filter((line) => line.startsWith("FOREIGN KEY"))
	for (const line of fkLines) {
		const match = line.match(
			/FOREIGN KEY \(`?(\w+)`?\) REFERENCES `?(\w+)`?\(`?(\w+)`?\)/
		)
		if (match) {
			const sourceColumn = match[1]
			const referencedTable = match[2]
			const referencedColumn = match[3]
			const constraint: ForeignKeyConstraint = {
				type: "constraint",
				constraintType: "foreignKey",
				name: "",
				sourceTable: tableName,
				sourceSchema: schema,
				sourceColumn,
				referencedTable,
				referencedSchema: schema, // Assuming same schema
				referencedColumn,
				rawSql: line
			}
			constraints.push(constraint)
		}
	}

	// Remove any foreign key constraints with "unknown" sourceColumn
	constraints = constraints.filter(
		(c) => !(c.constraintType === "foreignKey" && c.sourceColumn === "unknown")
	)

	tables[key] = {
		type: "table",
		name: tableName,
		schema,
		comment,
		rawSql,
		columns,
		constraints
	}
}

export function parseSqlDump(sqlContent: string): {
	database: Database
	failedStatements: string[]
} {
	// Step 1: Remove all -- style comments (single-line or multi-line)
	let cleanedSql = sqlContent
		.replace(/--.*$/gm, "") // Remove single-line comments starting with --
		.replace(/\n\s*\n/g, "\n") // Remove extra newlines left after comment removal
		.trim()

	// Step 2: Remove COMMENT clauses from field and table definitions
	// This matches COMMENT, the quoted string (handling both single and double quotes),
	// and preserves the terminating comma or semicolon
	cleanedSql = cleanedSql
		.replace(/COMMENT\s+(?:'[^']*'|"[^"]*")(\s*[,;])/gi, "$1")
		.replace(/COMMENT\s+(?:'[^']*'|"[^"]*")(\s*)$/gim, "$1")

	// Step 3: Split into statements by semicolon, filter out empty ones
	const statements = cleanedSql
		.split(";")
		.map((stmt) => stmt.trim())
		.filter((stmt) => stmt.length > 0)

	const tables: { [key: string]: Table } = {}
	const failedStatements: string[] = []

	for (const rawSql of statements) {
		// Since comments are already removed, use the rawSql as-is for parsing
		if (!rawSql.startsWith("CREATE TABLE")) {
			if (rawSql) {
				failedStatements.push(rawSql)
			}
			continue
		}

		// Preprocess to backtick `sql` and `usage` table names if not already backticked
		let processedSql = rawSql
		processedSql = processedSql.replace(
			/CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+(?!`)(sql|usage)(?![`\w])/gi,
			"CREATE TABLE `$1`"
		)
		processedSql = processedSql.replace(
			/REFERENCES\s+(?!`)(sql|usage)(?=\s*\()/gi,
			"REFERENCES `$1`"
		)

		try {
			const parseResult = parser.parse(processedSql, {
				database: "mysql"
			}) as ParseResult
			const ast = parseResult.ast

			if (ast.type === "create" && ast.keyword === "table") {
				// Pass an empty comment since we've stripped them all
				processCreateTable(ast, processedSql, "", tables)
			} else {
				failedStatements.push(processedSql)
			}
		} catch (error) {
			console.warn(
				`Error parsing statement: ${processedSql.substring(0, 50)}...\nError: ${(error as Error).message}`
			)
			failedStatements.push(processedSql)
		}
	}

	const database: Database = {
		type: "database",
		dialect: "mysql",
		tables: Object.values(tables)
	}

	return { database, failedStatements }
}

function calculateStatistics(database: Database, failedStatements: string[]) {
	const totalTables = database.tables.length
	const totalColumns = database.tables.reduce(
		(sum, table) => sum + table.columns.length,
		0
	)
	const constraints = database.tables.flatMap((table) => table.constraints)
	const totalConstraints = constraints.length
	const primaryKeyConstraints = constraints.filter(
		(c) => c.constraintType === "primaryKey"
	).length
	const foreignKeyConstraints = constraints.filter(
		(c) => c.constraintType === "foreignKey"
	).length
	const uniqueConstraints = constraints.filter(
		(c) => c.constraintType === "unique"
	).length
	const failedStatementsCount = failedStatements.length

	return {
		totalTables,
		totalColumns,
		totalConstraints,
		primaryKeyConstraints,
		foreignKeyConstraints,
		uniqueConstraints,
		failedStatementsCount
	}
}

function printStatistics(stats: ReturnType<typeof calculateStatistics>) {
	console.log("\nParsing Statistics:")
	console.log(`Found ${stats.totalTables} tables`)
	console.log(`Found ${stats.totalColumns} columns`)
	console.log(`Found ${stats.totalConstraints} constraints total:`)
	console.log(`  - ${stats.primaryKeyConstraints} primary key constraints`)
	console.log(`  - ${stats.foreignKeyConstraints} foreign key constraints`)
	console.log(`  - ${stats.uniqueConstraints} unique constraints`)
	console.log(`Failed statements: ${stats.failedStatementsCount}`)
}

if (require.main === module) {
	const args = process.argv.slice(2)
	const inputFile = args[0]
	const dumpJsonAst = args.includes("--dump-json-ast")
	const dumpTableName = args.includes("--dump-table")
		? args[args.indexOf("--dump-table") + 1]
		: null
	const dumpFailedOnly = args.includes("--dump-failed-only")
	const dumpParsedDatabase = args.includes("--dump-parsed-database")

	if (!inputFile) {
		console.error("Please provide an input SQL file")
		console.error(
			"Usage: bun process-mysql.ts <input.sql> [--dump-table <table_name>] [--dump-failed-only] [--dump-json-ast] [--dump-parsed-database]"
		)
		process.exit(1)
	}

	try {
		const sqlContent = fs.readFileSync(path.resolve(inputFile), "utf-8")

		if (dumpJsonAst) {
			const cleanedSql = sqlContent.replace(/--.*$/gm, "").trim()
			const statements = cleanedSql
				.split(";")
				.map((stmt) => stmt.trim())
				.filter((stmt) => stmt.length > 0)
			for (let stmt of statements) {
				if (stmt.startsWith("CREATE TABLE")) {
					stmt = stmt.replace(
						/CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+(?!`)(sql|usage)(?![`\w])/gi,
						"CREATE TABLE `$1`"
					)
					stmt = stmt.replace(
						/REFERENCES\s+(?!`)(sql|usage)(?=\s*\()/gi,
						"REFERENCES `$1`"
					)
				}
				try {
					const parseResult = parser.parse(stmt, { database: "mysql" })
					console.log(`AST for statement:\n${stmt}\n`)
					console.log(JSON.stringify(parseResult.ast, null, 2))
				} catch (error) {
					console.error(
						`Error parsing statement:\n${stmt}\nError: ${(error as Error).message}`
					)
				}
			}
			process.exit(0)
		}

		const { database, failedStatements } = parseSqlDump(sqlContent)

		if (dumpTableName) {
			const table = database.tables.find(
				(t) =>
					t.name === dumpTableName || `${t.schema}.${t.name}` === dumpTableName
			)
			if (table) {
				console.log(JSON.stringify(table, null, 2))
			} else {
				console.error(`Table "${dumpTableName}" not found`)
				process.exit(1)
			}
		} else if (dumpFailedOnly) {
			console.log("Failed statements:")
			console.log(JSON.stringify(failedStatements, null, 2))
		} else if (dumpParsedDatabase) {
			console.log(JSON.stringify({ database }, null, 2))
		} else {
			console.log("\nParsed Database:")
			console.log(JSON.stringify(database, null, 2))
			const stats = calculateStatistics(database, failedStatements)
			printStatistics(stats)
		}
	} catch (error) {
		console.error("Error:", (error as Error).message)
		process.exit(1)
	}
}
