import fs from "node:fs/promises"
import * as fsSync from "node:fs"
import * as path from "node:path"
import type { Database, ForeignKeyConstraint, Column } from "./relational"
import type { Mapping, ColumnMapping, SourceColumn } from "./mapping"
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

// Configuration interface for specifying hardcoded input tables
interface MappingConfig {
	inputTables: Record<string, string[]> // dbId (e.g., "tsql_0") to list of table keys (e.g., "dbo.User")
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
		{ destination: string; sources: string[]; description: string }[]
	>
): string {
	if (Object.keys(previousMappings).length === 0) {
		return "**All Previous Mappings:** None.\n"
	}
	let section = "**All Previous Mappings:**\n"
	for (const [tableKey, mappings] of Object.entries(previousMappings)) {
		section += `- ${tableKey}:\n`
		for (const mapping of mappings) {
			const sourcesStr = mapping.sources.map((s) => `"${s}"`).join(", ")
			section += `  - destination: ${mapping.destination}, sources: [${sourcesStr}], description: "${mapping.description}"\n`
		}
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

function buildSourceTablesSection(inDbs: Record<string, Database>): string {
	let section = "**Source Tables:**\n"
	for (const [dbId, db] of Object.entries(inDbs)) {
		for (const table of db.tables) {
			const tableKey = `${dbId}.${table.schema}.${table.name}`
			const typeMap =
				getDatabaseTablesTypeMap(db)[`${table.schema}.${table.name}`]
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
					section += `    - ${fk.sourceColumn} references ${dbId}.${fk.referencedSchema}.${fk.referencedTable}.${fk.referencedColumn}\n`
				}
			} else {
				section += "    - None\n"
			}
		}
	}
	return section
}

const mappingInstructions = `
**Instructions:**
- Map columns for the specified target table only, aligning with the TMForum SID data model for semantic consistency (e.g., 'mysql.customer' to SID 'Customer', 'mysql.product_offering' to 'ProductOffering').
- For each target column:
  - Identify source columns from 'Source Tables' with the same semantic meaning, listing them in 'sources' as '<dbId>.<table>.<column>', where <dbId> is the identifier of the source database (e.g., 'tsql_0', 'tsql_1', etc.). If none match, use 'sources: []'.
  - Provide a complete English description in 'description' explaining how the target column is derived from the source columns or other mechanisms.
  - If the target column is derived directly from source columns without transformation, state that it is a direct mapping.
  - If a transformation is required, describe the transformation in plain English, specifying how the source data is manipulated to produce the target value.
  - For primary keys without sources, describe that it is a surrogate key generated automatically, e.g., using an auto-increment mechanism.
  - For foreign keys without direct sources, describe that the value is obtained by looking up the referenced table based on the relationship.
  - For columns with no sources and no special function, describe that the value is set to null or a default value as appropriate.
  - Respect data types and constraints (e.g., 'NOT NULL'); mention any necessary type conversions in the description.
  - Avoid mapping ID fields (e.g., columns ending with 'ID') from the source to non-ID fields in the target. IDs typically represent keys, not descriptive data. For instance, do not map 'tsql_0.User.CreditRatingID' to 'mysql.party_credit_profile.creditRiskRating'; instead, map a descriptive field like 'tsql_0.CreditRating.Name' if available.
  - Ensure that the 'description' is a plain English explanation and does not include any code or JavaScript expressions.
- Every target column must be included in the output, even if unmapped (use 'sources: []' and an appropriate 'description').
- Focus solely on column mappings and descriptions; do not specify joins or include implementation details.
`

const outputFormat = `
**Output Format:**
\`\`\`json
{
  "mappings": [
    {
      "destination": "mysql.target_table.column",
      "sources": ["tsql_0.source_table.source_column"],
      "description": "A complete English description of the transformation."
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
  { "destination": "mysql.customer.name", "sources": ["tsql_0.users.full_name"], "description": "The 'name' field in the 'customer' table is directly mapped from the 'full_name' field in the 'users' table of the source database 'tsql_0'." }
  \`\`\`
- Transformation (concatenation):
  \`\`\`json
  { "destination": "mysql.party_name.full_name", "sources": ["tsql_0.contact.first_name", "tsql_0.contact.last_name"], "description": "The 'full_name' field is created by concatenating the 'first_name' and 'last_name' from the 'contact' table in the source database 'tsql_0', with a space in between." }
  \`\`\`
- Generated surrogate key:
  \`\`\`json
  { "destination": "mysql.product_offering.id", "sources": [], "description": "The 'id' field is a surrogate primary key that is automatically generated using an auto-increment mechanism, as there is no corresponding source column." }
  \`\`\`
- Foreign key lookup:
  \`\`\`json
  { "destination": "mysql.party_role_association.party_role_id", "sources": [], "description": "The 'party_role_id' field references the 'id' of the 'party_role' table. Since there is no direct source column, the value is obtained by looking up the appropriate 'party_role' record based on the defined relationship." }
  \`\`\`
- Unmapped column:
  \`\`\`json
  { "destination": "mysql.customer_billing_account.notes", "sources": [], "description": "There is no corresponding source column for 'notes', so this field is set to null." }
  \`\`\`
- Type conversion:
  \`\`\`json
  { "destination": "mysql.customer.created_date", "sources": ["tsql_0.users.signup_date"], "description": "The 'created_date' field is derived from the 'signup_date' in the source database 'tsql_0', which is a timestamp. It is converted to a string in ISO format to match the target column's data type." }
  \`\`\`
`

const badExamples = `
**Bad Examples (Avoid These):**
- Incorrect mapping of ID to non-ID field:
  \`\`\`json
  { "destination": "mysql.party_credit_profile.creditRiskRating", "sources": ["tsql_0.User.CreditRatingID"], "description": "Direct mapping from 'tsql_0.User.CreditRatingID' to 'mysql.party_credit_profile.creditRiskRating'." }
  \`\`\`
  **Reasoning:** 'tsql_0.User.CreditRatingID' is an ID field, likely a foreign key, and should not be directly mapped to 'creditRiskRating', which is probably a descriptive field. Instead, the descriptive value from the referenced table (e.g., 'tsql_0.CreditRating.Name') should be mapped.
- Including code in description:
  \`\`\`json
  { "destination": "mysql.customer.name", "sources": ["tsql_0.users.full_name"], "description": "Set to arg[0]" }
  \`\`\`
  **Reasoning:** Descriptions should be in plain English, not using code-like expressions.
`

function generateMappingPromptForTable(
	outDb: Database,
	targetTableKey: string,
	inDbs: Record<string, Database>,
	previousMappings: Record<
		string,
		{ destination: string; sources: string[]; description: string }[]
	>
): string {
	const targetTableSection = `**Target Table to Map:**\n${buildTargetTablesSection(outDb, [targetTableKey])}`
	const relatedTablesSection = "**Related Tables for Context:** None.\n"
	const allPreviousMappingsSection =
		getAllPreviousMappingsSection(previousMappings)
	const sourceSection = buildSourceTablesSection(inDbs)
	const intro =
		"You are mapping columns from source databases to a target table in a telecom database following the TMForum SID data model."

	return `
${intro}

${targetTableSection}

${relatedTablesSection}

${allPreviousMappingsSection}

${sourceSection}

${mappingInstructions}

${outputFormat}

${example}

${badExamples}

Generate mappings for the target table "${targetTableKey}" only.
`
}

function parseDestination(
	destination: string,
	outputDb: Database
): { schema: string; table: string; column: string } | null {
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
	return { schema: table.schema, table: table.name, column: columnName }
}

function parseSource(
	source: string,
	inDbs: Record<string, Database>
): SourceColumn<string> | null {
	const parts = source.split(".")
	if (parts.length !== 3) {
		console.error(`Invalid source format: ${source}`)
		return null
	}
	const dbId = parts[0]
	if (!(dbId in inDbs)) {
		console.error(`Database id ${dbId} not found in input databases`)
		return null
	}
	const tableName = parts[1]
	const columnName = parts[2]
	const db = inDbs[dbId]
	const table = db.tables.find((t) => t.name === tableName)
	if (!table) {
		console.error(`Table ${tableName} not found in database ${dbId}`)
		return null
	}
	return {
		databaseId: dbId,
		schema: table.schema,
		table: table.name,
		column: columnName
	}
}

const MappingSchema = z.object({
	destination: z.string(),
	sources: z.array(z.string()),
	description: z.string()
})

const MappingsSchema = z.object({
	mappings: z.array(MappingSchema)
})

async function generateMappings(
	inputDbs: Database[],
	outputDb: Database,
	targetTables: { schema: string; table: string }[],
	config: MappingConfig
): Promise<Mapping<string>> {
	const inDbs: Record<string, Database> = {}
	inputDbs.forEach((db, index) => {
		inDbs[`tsql_${index}`] = db
	})

	// Create a filtered version of inDbs with only the specified tables
	const filteredInDbs: Record<string, Database> = {}
	for (const [dbId, db] of Object.entries(inDbs)) {
		const tablesToInclude = config.inputTables[dbId] || []
		const filteredTables = db.tables.filter((table) => {
			const tableKey = `${table.schema}.${table.name}`
			return tablesToInclude.includes(tableKey)
		})
		filteredInDbs[dbId] = { ...db, tables: filteredTables }
	}

	const tableKeys = targetTables.map((t) => `${t.schema}.${t.table}`)

	const previousMappings: Record<
		string,
		{ destination: string; sources: string[]; description: string }[]
	> = {}
	const allColumnMappings: ColumnMapping<string>[] = []

	let debugStream: fsSync.WriteStream | undefined
	if (process.env.DEBUG_OUTPUT) {
		const streamResult = await tryCatch(fs.open(process.env.DEBUG_OUTPUT, "a"))
		if (streamResult.error) {
			console.error("Error opening debug stream:", streamResult.error.message)
		} else {
			debugStream = fsSync.createWriteStream("", { fd: streamResult.data.fd })
		}
	}

	for (const tableKey of tableKeys) {
		const prompt = generateMappingPromptForTable(
			outputDb,
			tableKey,
			filteredInDbs, // Use filtered tables for the prompt
			previousMappings
		)
		console.error(`Generating mappings for table: ${tableKey}...`)
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
			console.error(
				`Error calling OpenAI API for table ${tableKey}:`,
				completionResult.error
			)
			if (debugStream) {
				debugStream.write(
					`Error calling OpenAI API for table ${tableKey}: ${completionResult.error.message}\n\n`
				)
			}
			continue
		}

		const completion = completionResult.data
		const responseContent = completion.choices[0].message.content
		if (responseContent) {
			const parseResult = trySync(() => JSON.parse(responseContent))
			if (parseResult.error) {
				console.error(
					`Error parsing response for table ${tableKey}:`,
					parseResult.error
				)
				console.error(`Raw response: ${responseContent}`)
				if (debugStream) {
					debugStream.write(
						`Error parsing response for table ${tableKey}: ${parseResult.error.message}\n\n`
					)
				}
			} else {
				const jsonResponse = parseResult.data
				const mappingsResult = trySync(() => MappingsSchema.parse(jsonResponse))
				if (mappingsResult.error) {
					console.error(
						`Error validating mappings for table ${tableKey}:`,
						mappingsResult.error
					)
					if (debugStream) {
						debugStream.write(
							`Error validating mappings for table ${tableKey}: ${mappingsResult.error.message}\n\n`
						)
					}
				} else {
					const mappings = mappingsResult.data.mappings
					for (const mapping of mappings) {
						const parsedDestination = parseDestination(
							mapping.destination,
							outputDb
						)
						if (!parsedDestination) {
							continue
						}
						const sources: SourceColumn<string>[] = []
						for (const sourceStr of mapping.sources) {
							const parsedSource = parseSource(sourceStr, inDbs) // Use original inDbs for validation
							if (parsedSource) {
								sources.push(parsedSource)
							}
						}
						const columnMapping: ColumnMapping<string> = {
							sources,
							destinationSchema: parsedDestination.schema,
							destinationTable: parsedDestination.table,
							destinationColumn: parsedDestination.column,
							description: mapping.description
						}
						allColumnMappings.push(columnMapping)
					}
					previousMappings[tableKey] = mappings
					if (debugStream) {
						const mappingEntry = { table: tableKey, mappings }
						debugStream.write(`${JSON.stringify(mappingEntry, null, 2)}\n\n`)
					}
					console.error(
						`Processed mappings for ${tableKey}, added ${mappings.length} column mappings`
					)
				}
			}
		} else {
			console.error(`No content received for table ${tableKey}`)
			if (debugStream) {
				debugStream.write(`No content received for table ${tableKey}\n\n`)
			}
		}
	}

	const finalMapping: Mapping<string> = {
		type: "mapping",
		in: inDbs,
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
		const inputDbs = [inputDb]
		const targetTables = [
			{ schema: "mysql", table: "party_profile" },
			{ schema: "mysql", table: "party_role_association" },
			{ schema: "mysql", table: "party_role_category" },
			{ schema: "mysql", table: "party_role_currency" },
			{ schema: "mysql", table: "party_payment" },
			{ schema: "mysql", table: "party_role" },
			{ schema: "mysql", table: "party_credit_profile" },
			{ schema: "mysql", table: "party_name" },
			{ schema: "mysql", table: "party" }
		]
		const config: MappingConfig = {
			inputTables: {
				tsql_0: [
					"dbo.User",
					"dbo.Contact",
					"dbo.UserContactConnector",
					"dbo.ContactType",
					"dbo.UserPaymentMethod",
					"dbo.PaymentType",
					"dbo.Invoice",
					"dbo.StatementDetails",
					"dbo.UserInvoicer",
					"dbo.BillGroup",
					"dbo.InvoiceConfiguration",
					"dbo.UserStatusType",
					"dbo.StatusType",
					"dbo.CreditRating",
					"dbo.Owner",
					"dbo.UserOwner",
					"dbo.UserParent",
					"dbo.UserPackage",
					"dbo.UserService",
					"dbo.Package",
					"dbo.Service"
				]
			}
		}
		const mapping = await generateMappings(
			inputDbs,
			outputDb,
			targetTables,
			config
		)
		console.log(JSON.stringify(mapping, null, 2))
	})()
}

export { generateMappings }
