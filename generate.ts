import fs from "node:fs/promises"
import * as fsSync from "node:fs"
import * as path from "node:path"
import type { Database, ForeignKeyConstraint, Column } from "./relational"
import type { Mapping, ColumnMapping } from "./mapping"
import dotenv from "dotenv"
import OpenAI from "openai"
import { z } from "zod"
import { tryCatch, trySync } from "./try-catch"

dotenv.config()

if (!process.env.OPENAI_API_KEY) {
	console.error("Error: OPENAI_API_KEY environment variable is not set")
	process.exit(1)
}

const openai = new OpenAI({
	apiKey: process.env.OPENAI_API_KEY
})

const IN_DATABASE_PATH = "logisense.tsql.json"
const OUT_DATABASE_PATH = "sid.mysql.json"

// Configuration interface with a single array of source tables
interface MappingConfig {
	inputTables: string[] // e.g., ["dbo.User", "dbo.Contact"]
	outputTables?: string[] // Optional list of target table keys
}

async function readJsonFile(filePath: string): Promise<Database> {
	const contentResult = await tryCatch(
		fs.readFile(path.resolve(filePath), "utf-8")
	)
	if (contentResult.error) {
		throw new Error(
			`Failed to read file ${filePath}: ${contentResult.error.message}`
		)
	}
	const content = contentResult.data
	const parseResult = trySync(() => JSON.parse(content))
	if (parseResult.error) {
		throw new Error(
			`Failed to parse JSON from ${filePath}: ${parseResult.error.message}`
		)
	}
	const parsedJson = parseResult.data
	const database = parsedJson.database || (parsedJson as Database)
	const validateResult = trySync(() => validateDatabase(database))
	if (validateResult.error) {
		throw new Error(
			`Invalid database structure in ${filePath}: ${validateResult.error.message}`
		)
	}
	return database
}

function validateDatabase(database: Database): Database {
	if (database.type !== "database") {
		throw new Error(
			"Invalid database structure: missing 'type' property or not 'database'"
		)
	}
	if (!database.dialect || !["tsql", "mysql"].includes(database.dialect)) {
		throw new Error(
			"Invalid database structure: missing or invalid 'dialect' property"
		)
	}
	if (!Array.isArray(database.tables)) {
		throw new Error("Invalid database structure: 'tables' must be an array")
	}
	for (const table of database.tables) {
		if (table.type !== "table") {
			throw new Error(
				`Invalid table structure in table: ${table.name || "unknown"}`
			)
		}
		if (!table.columns || !Array.isArray(table.columns)) {
			throw new Error(
				`Missing or invalid columns in table: ${table.name || "unknown"}`
			)
		}
		if (!table.constraints || !Array.isArray(table.constraints)) {
			throw new Error(
				`Missing or invalid constraints in table: ${table.name || "unknown"}`
			)
		}
	}
	return database
}

export function getDatabaseTablesMap(
	database: Database
): Record<string, string[]> {
	const result: Record<string, string[]> = {}
	for (const table of database.tables) {
		const tableKey = `${table.schema}.${table.name}`
		result[tableKey] = table.columns.map((column) => column.name)
	}
	return result
}

export function getDatabaseTablesTypeMap(
	database: Database
): Record<string, Record<string, string>> {
	const result: Record<string, Record<string, string>> = {}
	for (const table of database.tables) {
		const tableKey = `${table.schema}.${table.name}`
		result[tableKey] = {}
		for (const column of table.columns) {
			result[tableKey][column.name] = formatColumnTypeString(column)
		}
	}
	return result
}

function formatColumnTypeString(column: Column): string {
	let typeString = column.dataType.toUpperCase()
	switch (column.typeInfo.kind) {
		case "varchar":
		case "nvarchar":
		case "char":
			if (column.typeInfo.length !== null) {
				typeString += `(${column.typeInfo.length})`
			}
			break
		case "numeric":
		case "decimal":
			if (column.typeInfo.precision !== null) {
				typeString += `(${column.typeInfo.precision}`
				if (column.typeInfo.scale !== null) {
					typeString += `,${column.typeInfo.scale}`
				}
				typeString += ")"
			}
			break
		case "datetime":
		case "money":
		case "bit":
		case "text":
		case "simple":
		case "boolean":
		case "integer":
			break
	}
	if (!column.isNullable) {
		typeString += " NOT NULL"
	}
	if (column.defaultValue !== undefined) {
		typeString += ` DEFAULT ${column.defaultValue}`
	}
	return typeString
}

function getAllPreviousMappingsSection(
	previousMappings: Record<
		string,
		{ source: string; destination: string; description: string }[]
	>
): string {
	const allMappings = Object.values(previousMappings).flat()
	if (allMappings.length === 0) {
		return "**All Previous Mappings:** None.\n"
	}
	let section = "**All Previous Mappings:**\n"
	for (const mapping of allMappings) {
		section += `- source: "${mapping.source}", destination: "${mapping.destination}", description: "${mapping.description}"\n`
	}
	return section
}

function buildTargetTablesSection(
	outDb: Database,
	tableKeys: string[]
): string {
	let section = ""
	for (const tableKey of tableKeys) {
		const table = outDb.tables.find((t) => `${t.schema}.${t.name}` === tableKey)
		if (!table) {
			throw new Error(`Table ${tableKey} not found in outDb`)
		}
		const typeMap = getDatabaseTablesTypeMap(outDb)[tableKey]
		const fkConstraints = table.constraints.filter(
			(c): c is ForeignKeyConstraint => c.constraintType === "foreignKey"
		)
		section += `- ${tableKey}:\n  - Columns:\n`
		for (const [col, type] of Object.entries(typeMap)) {
			section += `    - ${col}: ${type}\n`
		}
		section += "  - Foreign Key Constraints:\n"
		if (fkConstraints.length > 0) {
			for (const fk of fkConstraints) {
				section += `    - ${fk.sourceColumn} references ${fk.referencedSchema}.${fk.referencedTable}.${fk.referencedColumn}\n`
			}
		} else {
			section += "    - None\n"
		}
	}
	return section
}

function buildSourceTablesSection(
	inputDb: Database,
	sourceTableKeys: string[]
): string {
	let section = "**Source Tables to Map:**\n"
	for (const tableKey of sourceTableKeys) {
		const table = inputDb.tables.find(
			(t) => `${t.schema}.${t.name}` === tableKey
		)
		if (!table) {
			console.warn(`Table ${tableKey} not found in input database`)
			continue
		}
		const typeMap = getDatabaseTablesTypeMap(inputDb)[tableKey]
		const fkConstraints = table.constraints.filter(
			(c): c is ForeignKeyConstraint => c.constraintType === "foreignKey"
		)
		section += `- ${tableKey}:\n  - Columns:\n`
		for (const [col, type] of Object.entries(typeMap)) {
			section += `    - ${col}: ${type}\n`
		}
		section += "  - Foreign Key Constraints:\n"
		if (fkConstraints.length > 0) {
			for (const fk of fkConstraints) {
				section += `    - ${fk.sourceColumn} references ${fk.referencedSchema}.${fk.referencedTable}.${fk.referencedColumn}\n`
			}
		} else {
			section += "    - None\n"
		}
	}
	return section
}

const mappingInstructions = `
**Instructions:**
- Map the columns from the specified 'Source Tables to Map' forward to the TMForum SID data model in the target schema.
- For each column in the source tables:
  - Identify **one** target column in the 'All Output Tables' section that best matches its semantic meaning, aligning with the TMForum SID data model (e.g., 'dbo.User.Account' might map to 'mysql.party.ID').
  - Provide the source as '<schema>.<table>.<column>' (e.g., 'dbo.User.Account').
  - Provide the destination as '<schema>.<table>.<column>' (e.g., 'mysql.party.ID').
  - If no suitable target column exists, set 'destination' to an empty string and explain in 'description' why it’s unmapped (e.g., not relevant to SID schema).
  - In 'description', provide a complete English explanation of how the source column maps to the target column or why it doesn’t map, including any necessary transformations (e.g., type conversion, concatenation).
- Respect data types and constraints (e.g., 'NOT NULL'); mention any necessary type conversions in the description.
- Avoid mapping ID fields (e.g., columns ending with 'ID') from the source to non-ID fields in the target unless semantically appropriate. IDs typically represent keys, not descriptive data.
- Ensure that the 'description' is a plain English explanation and does not include any code or JavaScript expressions.
- Every source column must be included in the output, even if unmapped (use 'destination': '' and an appropriate 'description').
- Focus solely on column mappings and descriptions; do not specify joins or include implementation details.
`

const outputFormat = `
**Output Format:**
\`\`\`json
{
  "mappings": [
    {
      "source": "dbo.source_table.source_column",
      "destination": "mysql.target_table.column",
      "description": "A complete English description of the mapping or reason for no mapping."
    },
    ...
  ]
}
\`\`\`
`

const example = `
**Examples:**
- Direct mapping:
  \`\`\`json
  { "source": "dbo.User.Account", "destination": "mysql.party.ID", "description": "The 'Account' field from 'dbo.User' directly maps to the 'ID' field in the 'party' table as a unique identifier." }
  \`\`\`
- Type conversion:
  \`\`\`json
  { "source": "dbo.User.CreatedDate", "destination": "mysql.party_profile.dateCreated", "description": "The 'CreatedDate' field from 'dbo.User', a DATETIME, maps to 'dateCreated' in 'party_profile', converted to a string in ISO format." }
  \`\`\`
- Unmapped column:
  \`\`\`json
  { "source": "dbo.Contact.InternalNote", "destination": "", "description": "The 'InternalNote' field from 'dbo.Contact' has no corresponding column in the SID schema and is not mapped." }
  \`\`\`
`

const badExamples = `
**Bad Examples (Avoid These):**
- Incorrect ID mapping:
  \`\`\`json
  { "source": "dbo.User.CreditRatingID", "destination": "mysql.party_credit_profile.creditRiskRating", "description": "Maps 'CreditRatingID' to 'creditRiskRating'." }
  \`\`\`
  **Reasoning:** 'CreditRatingID' is an ID field and should not map to 'creditRiskRating', a descriptive field. Map to a key field or use a descriptive field like 'dbo.CreditRating.Name'.
- Code in description:
  \`\`\`json
  { "source": "dbo.User.Account", "destination": "mysql.party.ID", "description": "Set to arg[0]" }
  \`\`\`
  **Reasoning:** Descriptions must be plain English, not code-like expressions.
`

function generateMappingPromptForSourceTables(
	outDb: Database,
	inputDb: Database,
	sourceTableKeys: string[],
	previousMappings: Record<
		string,
		{ source: string; destination: string; description: string }[]
	>,
	outputTables?: string[]
): string {
	const targetTableKeys =
		outputTables || outDb.tables.map((t) => `${t.schema}.${t.name}`)
	const allOutputTablesSection = `**All Output Tables:**\n${buildTargetTablesSection(outDb, targetTableKeys)}`
	const sourceSection = buildSourceTablesSection(inputDb, sourceTableKeys)
	const allPreviousMappingsSection =
		getAllPreviousMappingsSection(previousMappings)
	const intro =
		"You are a data scientist specializing in mapping application-specific data structures to TM Forum APIs. As an expert in TM Forum standards, OpenAPI, and SID models, you excel at interpreting and aligning data models. Your approach involves analyzing documentation provided for application-specific data structures, carefully considering both field names and descriptions to create precise mappings. You strictly adhere to available documentation, avoiding assumptions about fields that might exist but are not documented."

	return `
${intro}

${allOutputTablesSection}

${sourceSection}

${allPreviousMappingsSection}

${mappingInstructions}

${outputFormat}

${example}

${badExamples}

Generate mappings for all columns in the specified source tables listed in 'Source Tables to Map'.
`
}

function parseDestination(
	destination: string,
	outputDb: Database
): { schema: string; table: string; column: string } | null {
	if (!destination) {
		return null
	}
	const parts = destination.split(".")
	if (parts.length !== 3 || parts[0] !== "mysql") {
		console.error(`Invalid destination format: ${destination}`)
		return null
	}
	const tableName = parts[1]
	const columnName = parts[2]
	const table = outputDb.tables.find((t) => t.name === tableName)
	if (!table) {
		console.error(`Table ${tableName} not found in output database`)
		return null
	}
	return { schema: table.schema, table: tableName, column: columnName }
}

function parseSource(
	source: string,
	inputDb: Database
): { schema: string; table: string; column: string } | null {
	const parts = source.split(".")
	if (parts.length !== 3) {
		console.error(
			`Invalid source format: ${source} (expected 3 parts: schema.table.column)`
		)
		return null
	}
	const [schema, tableName, columnName] = parts
	const table = inputDb.tables.find(
		(t) => t.schema === schema && t.name === tableName
	)
	if (!table) {
		console.error(`Table ${schema}.${tableName} not found in input database`)
		return null
	}
	return { schema, table: tableName, column: columnName }
}

const MappingSchema = z.object({
	source: z.string(),
	destination: z.string(),
	description: z.string()
})

const MappingsSchema = z.object({
	mappings: z.array(MappingSchema)
})

async function generateMappings(
	inputDb: Database,
	outputDb: Database,
	config: MappingConfig
): Promise<Mapping> {
	// Filter the input database to only include specified source tables
	const filteredInputDb: Database = {
		...inputDb,
		tables: inputDb.tables.filter((table) => {
			const tableKey = `${table.schema}.${table.name}`
			return config.inputTables.includes(tableKey)
		})
	}

	const sourceTableKeys = config.inputTables
	const previousMappings: Record<
		string,
		{ source: string; destination: string; description: string }[]
	> = {}
	const allColumnMappings: ColumnMapping[] = []

	let debugStream: fsSync.WriteStream | undefined
	if (process.env.DEBUG_OUTPUT) {
		const streamResult = await tryCatch(fs.open(process.env.DEBUG_OUTPUT, "a"))
		if (streamResult.error) {
			console.error("Error opening debug stream:", streamResult.error.message)
		} else {
			debugStream = fsSync.createWriteStream("", { fd: streamResult.data.fd })
		}
	}

	// Generate mappings for all source tables in one go
	const prompt = generateMappingPromptForSourceTables(
		outputDb,
		filteredInputDb,
		sourceTableKeys,
		previousMappings,
		config.outputTables
	)
	console.error("Generating mappings for all specified source tables...")
	console.error("Prompt being sent to LLM:")
	console.error("----------------------------------------")
	console.error(prompt)
	console.error("----------------------------------------")

	const completionResult = await tryCatch(
		openai.chat.completions.create({
			model: "o3-mini",
			messages: [{ role: "user", content: prompt }],
			response_format: { type: "json_object" }
		})
	)

	if (completionResult.error) {
		console.error("Error calling OpenAI API:", completionResult.error)
		if (debugStream) {
			debugStream.write(
				`Error calling OpenAI API: ${completionResult.error.message}\n\n`
			)
		}
	} else {
		const completion = completionResult.data
		const responseContent = completion.choices[0].message.content
		if (responseContent) {
			const parseResult = trySync(() => JSON.parse(responseContent))
			if (parseResult.error) {
				console.error("Error parsing response:", parseResult.error)
				console.error(`Raw response: ${responseContent}`)
				if (debugStream) {
					debugStream.write(
						`Error parsing response: ${parseResult.error.message}\n\n`
					)
				}
			} else {
				const jsonResponse = parseResult.data
				const mappingsResult = trySync(() => MappingsSchema.parse(jsonResponse))
				if (mappingsResult.error) {
					console.error("Error validating mappings:", mappingsResult.error)
					if (debugStream) {
						debugStream.write(
							`Error validating mappings: ${mappingsResult.error.message}\n\n`
						)
					}
				} else {
					const mappings = mappingsResult.data.mappings
					for (const mapping of mappings) {
						const parsedSource = parseSource(mapping.source, inputDb)
						if (!parsedSource) {
							continue
						}
						const parsedDestination = mapping.destination
							? parseDestination(mapping.destination, outputDb)
							: null
						const columnMapping: ColumnMapping = {
							sourceSchema: parsedSource.schema,
							sourceTable: parsedSource.table,
							sourceColumn: parsedSource.column,
							destinationSchema: parsedDestination
								? parsedDestination.schema
								: "",
							destinationTable: parsedDestination
								? parsedDestination.table
								: "",
							destinationColumn: parsedDestination
								? parsedDestination.column
								: "",
							description: mapping.description
						}
						allColumnMappings.push(columnMapping)
					}
					// Store mappings under a generic key since we’re mapping all source tables at once
					previousMappings.source_tables = mappings
					if (debugStream) {
						const mappingEntry = { tables: sourceTableKeys, mappings }
						debugStream.write(`${JSON.stringify(mappingEntry, null, 2)}\n\n`)
					}
					console.error(
						`Processed mappings for source tables, added ${mappings.length} column mappings`
					)
				}
			}
		} else {
			console.error("No content received from OpenAI API")
			if (debugStream) {
				debugStream.write("No content received from OpenAI API\n\n")
			}
		}
	}

	const finalMapping: Mapping = {
		type: "mapping",
		in: inputDb,
		out: outputDb,
		columnMappings: allColumnMappings
	}

	if (debugStream) {
		debugStream.end()
	}

	console.error("Mapping generation complete")
	return finalMapping
}

if (require.main === module) {
	;(async () => {
		const inputDb = await readJsonFile(IN_DATABASE_PATH)
		const outputDb = await readJsonFile(OUT_DATABASE_PATH)
		const config: MappingConfig = {
			inputTables: ["dbo.User"]
		}
		const mapping = await generateMappings(inputDb, outputDb, config)
		console.log(JSON.stringify(mapping, null, 2))
	})()
}

export { generateMappings }
