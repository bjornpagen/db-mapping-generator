import * as fs from "node:fs";
import * as path from "node:path";
import type { AST, Create, TableColumnAst } from "node-sql-parser";
import { Parser } from "node-sql-parser/build/transactsql";
import {
	buildLexer,
	tok,
	seq,
	alt,
	opt,
	rep,
	apply,
	expectEOF,
	expectSingleResult,
} from "typescript-parsec";
import type { Parser as TParser, Token } from "typescript-parsec";

// Import types from types.ts
import type {
	Database,
	Table,
	Column,
	ColumnTypeInfo,
	Constraint,
	ForeignKeyConstraint,
	DefaultConstraint,
	UniqueConstraint,
	PrimaryKeyConstraint,
	CheckConstraint,
} from "./relational";

const parser = new Parser();

// Define the lexer for SQL tokens
const lexer = buildLexer([
	[false, /^\s+/g, "WHITESPACE"],
	[true, /^ALTER\b/gi, "ALTER"],
	[true, /^TABLE\b/gi, "TABLE"],
	[true, /^WITH\b/gi, "WITH"],
	[true, /^CHECK\b/gi, "CHECK"],
	[true, /^ADD\b/gi, "ADD"],
	[true, /^CONSTRAINT\b/gi, "CONSTRAINT"],
	[true, /^FOREIGN\b/gi, "FOREIGN"],
	[true, /^KEY\b/gi, "KEY"],
	[true, /^REFERENCES\b/gi, "REFERENCES"],
	[true, /^DEFAULT\b/gi, "DEFAULT"],
	[true, /^FOR\b/gi, "FOR"],
	[true, /^UNIQUE\b/gi, "UNIQUE"],
	[true, /^\(/g, "LPAREN"],
	[true, /^\)/g, "RPAREN"],
	[true, /^\[.*?\]/g, "IDENTIFIER"],
	[true, /^[^ \t\r\n\[\]()]+/g, "OTHER"],
]);

// Type for column list result
type ColumnList = [
	Token<string>,
	[Token<string>, Token<string> | undefined][],
	Token<string>,
];

// Table name parser for schema-qualified and simple names
const tableNameParser = apply(
	alt(
		seq(tok("IDENTIFIER"), tok("OTHER"), tok("IDENTIFIER")),
		tok("IDENTIFIER"),
	),
	(result) => {
		if (Array.isArray(result) && result.length === 3) {
			return `${result[0].text}.${result[2].text}`;
		}
		if (!Array.isArray(result)) {
			return result.text;
		}
		throw new Error("Invalid table name format");
	},
);

// Parser for column list: ( [column_name], ... )
const columnListParser = seq(
	tok("LPAREN"),
	rep(seq(tok("IDENTIFIER"), opt(tok("OTHER")))),
	tok("RPAREN"),
);

// Foreign key parser with validation
const fkParser = seq(
	tok("ALTER"),
	tok("TABLE"),
	tableNameParser,
	opt(seq(tok("WITH"), tok("CHECK"))),
	tok("ADD"),
	tok("CONSTRAINT"),
	tok("IDENTIFIER"),
	tok("FOREIGN"),
	tok("KEY"),
	columnListParser,
	tok("REFERENCES"),
	tableNameParser,
	columnListParser,
);

// CHECK constraint parser
const checkParser = seq(
	tok("ALTER"),
	tok("TABLE"),
	tableNameParser,
	opt(seq(tok("WITH"), tok("CHECK"))),
	tok("ADD"),
	tok("CONSTRAINT"),
	tok("IDENTIFIER"),
	tok("CHECK"),
	tok("LPAREN"),
	rep(alt(tok("IDENTIFIER"), tok("OTHER"), tok("LPAREN"), tok("RPAREN"))),
	tok("RPAREN"),
);

// Default constraint parser
const defaultParser = seq(
	tok("ALTER"),
	tok("TABLE"),
	tableNameParser,
	tok("ADD"),
	tok("CONSTRAINT"),
	tok("IDENTIFIER"),
	tok("DEFAULT"),
	rep(alt(tok("LPAREN"), tok("RPAREN"), tok("OTHER"))),
	tok("FOR"),
	tok("IDENTIFIER"),
);

// Unique constraint parser
const uniqueParser = seq(
	tok("ALTER"),
	tok("TABLE"),
	tableNameParser,
	opt(seq(tok("WITH"), tok("CHECK"))),
	tok("ADD"),
	tok("CONSTRAINT"),
	tok("IDENTIFIER"),
	tok("UNIQUE"),
	columnListParser,
);

// Parse functions with validation
const parseFKConstraint = apply(fkParser, (tokens) => {
	const tableName = tokens[2] as string;
	const constraintName = (tokens[6] as Token<string>).text.slice(1, -1);
	const columns = (tokens[9] as ColumnList)[1].map((t) =>
		t[0].text.slice(1, -1),
	);
	const referencedTable = tokens[11] as string;
	const referencedColumns = (tokens[12] as ColumnList)[1].map((t) =>
		t[0].text.slice(1, -1),
	);

	if (columns.length === 0 || referencedColumns.length === 0) {
		throw new Error(
			`Invalid foreign key constraint: missing columns in ${constraintName}`,
		);
	}

	const [schema, table] = tableName
		.split(".")
		.map((s: string) => s.trim().slice(1, -1));
	const [refSchema, refTable] = referencedTable
		.split(".")
		.map((s: string) => s.trim().slice(1, -1));

	return {
		type: "constraint",
		constraintType: "foreignKey",
		name: constraintName,
		sourceTable: table,
		sourceSchema: schema || "dbo",
		sourceColumn: columns[0], // Assuming single-column FK for simplicity
		referencedTable: refTable,
		referencedSchema: refSchema || "dbo",
		referencedColumn: referencedColumns[0],
		rawSql: tokens
			.map((t) => {
				if (typeof t === "string") {
					return t;
				}
				if (Array.isArray(t)) {
					if (t.length > 0) {
						if (Array.isArray(t[0])) {
							return t
								.map((innerArray) => {
									if (Array.isArray(innerArray)) {
										return innerArray
											.map((item) => (item && "text" in item ? item.text : ""))
											.join(" ");
									}
									return innerArray && "text" in innerArray
										? innerArray.text
										: "";
								})
								.join(" ");
						}
						return t
							.map((item) => (item && "text" in item ? item.text : ""))
							.join(" ");
					}
					return "";
				}
				return t && "text" in t ? t.text : "";
			})
			.join(" "),
	} as ForeignKeyConstraint;
});

const parseCheckConstraint = apply(checkParser, (tokens) => {
	const tableName = tokens[2] as string;
	const constraintName = (tokens[6] as Token<string>).text.slice(1, -1);
	const conditionTokens = tokens[9] as Token<string>[];
	const condition = conditionTokens.map((t) => t.text).join(" ");

	const [schema, table] = tableName
		.split(".")
		.map((s: string) => s.trim().slice(1, -1));

	return {
		type: "constraint",
		constraintType: "check",
		name: constraintName,
		table,
		schema: schema || "dbo",
		expression: condition,
		rawSql: tokens
			.map((t) => {
				if (typeof t === "string") {
					return t;
				}
				if (Array.isArray(t)) {
					return t.map((x) => x.text).join(" ");
				}
				return t && "text" in t ? t.text : "";
			})
			.join(" "),
	} as CheckConstraint;
});

const parseDefaultConstraint = apply(defaultParser, (tokens) => {
	const tableName = tokens[2] as string;
	const constraintName = (tokens[5] as Token<string>).text.slice(1, -1);
	const defaultValueTokens = tokens[7] as Token<string>[];
	const defaultValue = defaultValueTokens.map((t) => t.text).join("");
	const columnName = (tokens[9] as Token<string>).text.slice(1, -1);

	const [schema, table] = tableName
		.split(".")
		.map((s: string) => s.trim().slice(1, -1));

	return {
		type: "constraint",
		constraintType: "default",
		name: constraintName,
		table,
		schema: schema || "dbo",
		column: columnName,
		value: defaultValue,
		rawSql: tokens
			.map((t) => {
				if (typeof t === "string") {
					return t;
				}
				if (Array.isArray(t)) {
					if (t.length > 0 && t[0] && "text" in t[0]) {
						return t.map((x) => x.text).join("");
					}
					return "";
				}
				return t.text;
			})
			.join(" "),
	} as DefaultConstraint;
});

const parseUniqueConstraint = apply(uniqueParser, (tokens) => {
	const tableName = tokens[2] as string;
	const constraintName = (tokens[6] as Token<string>).text.slice(1, -1);
	const columns = (tokens[8] as ColumnList)[1].map((t) =>
		t[0].text.slice(1, -1),
	);

	const [schema, table] = tableName
		.split(".")
		.map((s: string) => s.trim().slice(1, -1));

	return {
		type: "constraint",
		constraintType: "unique",
		name: constraintName,
		table,
		schema: schema || "dbo",
		columns,
		rawSql: tokens
			.map((t) => {
				if (typeof t === "string") {
					return t;
				}
				if (Array.isArray(t)) {
					if (t.length > 0) {
						if (Array.isArray(t[0])) {
							return t
								.map((innerArray) => {
									if (Array.isArray(innerArray)) {
										return innerArray
											.map((item) => (item && "text" in item ? item.text : ""))
											.join(" ");
									}
									return innerArray && "text" in innerArray
										? innerArray.text
										: "";
								})
								.join(" ");
						}
						return t
							.map((item) => (item && "text" in item ? item.text : ""))
							.join(" ");
					}
					return "";
				}
				return t && "text" in t ? t.text : "";
			})
			.join(" "),
	} as UniqueConstraint;
});

function preprocessSql(sql: string): string {
	let preprocessed = sql;
	// Map SQL Server-specific types to common SQL types
	preprocessed = preprocessed.replace(/\[ntext\]\s*/gi, "TEXT ");
	preprocessed = preprocessed.replace(/\[image\]\s*/gi, "TEXT ");
	preprocessed = preprocessed.replace(/\[xml\]\s*/gi, "TEXT ");
	preprocessed = preprocessed.replace(/\[varchar\]\s*\(max\)/gi, "TEXT");
	//preprocessed = preprocessed.replace(/\[nvarchar\]\s*\(max\)/gi, "TEXT")

	// Remove SQL Server-specific clauses
	preprocessed = preprocessed.replace(
		/WITH\s*\([^)]*\)\s*ON\s*\[[^\]]*\]/gi,
		"",
	);
	preprocessed = preprocessed.replace(/\s*TEXTIMAGE_ON\s*\[[^\]]*\]/gi, "");
	//preprocessed = preprocessed.replace(/\s*ON\s*\[[^\]]*\]/gi, "")

	// Remove ASC/DESC in column definitions as they are not necessary for parsing
	//preprocessed = preprocessed.replace(/\s+(ASC|DESC)/gi, "")

	// Additional cleanup: remove trailing commas and normalize spaces
	preprocessed = preprocessed.replace(/,\s*\)/g, ")");
	preprocessed = preprocessed.replace(/\s+/g, " ");

	return preprocessed.trim();
}

function parseConstraintWithParsec(
	stmt: string,
	tables: { [key: string]: Table },
): boolean {
	const tokensResult = lexer.parse(stmt);
	if (!tokensResult) {
		return false;
	}

	const tokenStream = tokensResult;
	let parser: TParser<string, Constraint> | null = null;

	if (stmt.includes("FOREIGN KEY")) {
		parser = parseFKConstraint;
	} else if (stmt.includes("DEFAULT")) {
		parser = parseDefaultConstraint;
	} else if (stmt.includes("UNIQUE")) {
		parser = parseUniqueConstraint;
	} else if (stmt.includes("CHECK") && !stmt.includes("FOREIGN KEY")) {
		parser = parseCheckConstraint;
	} else {
		return false;
	}

	try {
		const output = parser.parse(tokenStream);
		if (output.successful) {
			const eofResult = expectEOF(output);
			if (eofResult.successful) {
				const constraint = expectSingleResult(eofResult);
				let key: string;
				if (constraint.constraintType === "foreignKey") {
					key = `${(constraint as ForeignKeyConstraint).sourceSchema}.${(constraint as ForeignKeyConstraint).sourceTable}`;
				} else if (constraint.constraintType === "default") {
					key = `${(constraint as DefaultConstraint).schema}.${(constraint as DefaultConstraint).table}`;
				} else if (constraint.constraintType === "unique") {
					key = `${(constraint as UniqueConstraint).schema}.${(constraint as UniqueConstraint).table}`;
				} else if (constraint.constraintType === "check") {
					key = `${(constraint as CheckConstraint).schema}.${(constraint as CheckConstraint).table}`;
				} else {
					const constraintWithSchema = constraint as unknown as {
						schema: string;
						table: string;
					};
					key = `${constraintWithSchema.schema}.${constraintWithSchema.table}`;
				}

				if (tables[key]) {
					tables[key].constraints.push(constraint);
					return true;
				}
			}
		}
	} catch (error) {
		console.warn(`Failed to parse constraint: ${(error as Error).message}`);
		return false;
	}
	return false;
}

export function parseSqlDump(sqlContent: string): {
	database: Database;
	failedStatements: string[];
} {
	const opt = { database: "transactsql" };
	const batches = sqlContent.split(/\nGO\n/);
	const tables: { [key: string]: Table } = {};
	const failedStatements: string[] = [];
	let currentComment: string | null = null;

	for (const batch of batches) {
		const trimmedBatch = batch.trim();
		if (!trimmedBatch) {
			continue;
		}

		const statements = splitBatchIntoStatements(trimmedBatch);
		for (const stmt of statements) {
			const trimmedStmt = stmt.trim();
			if (trimmedStmt.startsWith("--") || trimmedStmt === "") {
				continue;
			}

			const commentMatch = trimmedStmt.match(
				/\/\*{6}\s+Object:\s+Table\s+\[([^\]]+)\]\.\[([^\]]+)\]\s+Script Date:[^\*]+\*{6}\//,
			);
			if (commentMatch) {
				currentComment = commentMatch[0];
				continue;
			}

			if (trimmedStmt.startsWith("CREATE TABLE")) {
				const preprocessedStmt = preprocessSql(trimmedStmt);
				try {
					const result = parser.parse(preprocessedStmt, opt);
					const astArray = normalizeAst(result);
					for (const ast of astArray) {
						if (isCreateTableAst(ast)) {
							processCreateTable(ast, trimmedStmt, currentComment, tables);
							currentComment = null;
						}
					}
				} catch {
					const table = parseCreateTableManually(trimmedStmt);
					if (table) {
						const key = `${table.schema}.${table.name}`;
						tables[key] = table;
					} else {
						failedStatements.push(trimmedStmt);
					}
				}
			} else if (trimmedStmt.startsWith("ALTER TABLE")) {
				const parsed = parseConstraintWithParsec(trimmedStmt, tables);
				if (!parsed) {
					failedStatements.push(trimmedStmt);
				}
			} else {
				failedStatements.push(trimmedStmt);
			}
		}
	}

	const database: Database = {
		type: "database",
		dialect: "tsql",
		tables: Object.values(tables),
	};

	return { database, failedStatements };
}

function splitBatchIntoStatements(batch: string): string[] {
	const lines = batch.split("\n");
	const statements: string[] = [];
	let currentStatement: string[] = [];

	for (const line of lines) {
		const trimmedLine = line.trim();
		if (
			trimmedLine.startsWith("CREATE TABLE") ||
			trimmedLine.startsWith("ALTER TABLE")
		) {
			if (currentStatement.length > 0) {
				statements.push(currentStatement.join("\n"));
			}
			currentStatement = [line];
		} else if (
			!trimmedLine.startsWith("--") &&
			!trimmedLine.startsWith("/*") &&
			trimmedLine !== ""
		) {
			currentStatement.push(line);
		}
	}
	if (currentStatement.length > 0) {
		statements.push(currentStatement.join("\n"));
	}
	return statements;
}

function normalizeAst(result: TableColumnAst): AST[] {
	const ast = result.ast;
	return Array.isArray(ast) ? ast : [ast];
}

function isCreateTableAst(ast: AST): ast is Create {
	return ast.type === "create" && ast.keyword === "table";
}

function processCreateTable(
	ast: Create,
	rawSql: string,
	comment: string | null,
	tables: { [key: string]: Table },
): void {
	if (!ast.table || !ast.table[0]) {
		console.warn(`No table info found in CREATE TABLE: ${rawSql}`);
		return;
	}

	const tableInfo = ast.table[0] as { db?: string; table: string };
	const schema = tableInfo.db || "dbo";
	const tableName = tableInfo.table;
	const key = `${schema}.${tableName}`;

	const columns: Column[] = [];
	const constraints: Constraint[] = [];

	for (const def of ast.create_definitions || []) {
		if (def.resource === "column") {
			columns.push(parseColumn(def as CreateColumnDefinition));
		} else if (def.resource === "constraint") {
			const constraint = parseConstraint(
				def as CreateConstraintDefinition,
				schema,
				tableName,
				rawSql,
			);
			if (constraint) {
				constraints.push(constraint);
			}
		}
	}

	tables[key] = {
		type: "table",
		name: tableName,
		schema,
		comment: comment || "",
		rawSql,
		columns,
		constraints,
		fileGroup: ast.index?.toString().match(/ON \[([^\]]+)\]/)?.[1] || undefined,
	};
}

interface CreateColumnDefinition {
	column: { column: string };
	resource: "column";
	nullable?: { type: string };
	definition: { dataType: string; length?: number | string; suffix?: string[] };
	default_val?: { value?: string | number | boolean };
}

interface CreateConstraintDefinition {
	resource: "constraint";
	constraint_type: string;
	constraint?: string;
	definition?:
		| Array<{ column: string }>
		| { expr: unknown }
		| { reference_definition: ReferenceDefinition };
}

interface ReferenceDefinition {
	table?: Array<{ db?: string; table: string }>;
	columns?: Array<{ column: string }>;
}

function parseColumn(def: CreateColumnDefinition): Column {
	const dataType = def.definition.dataType;
	const length = def.definition.length;
	const typeInfo = parseColumnTypeInfo(dataType, length);

	return {
		type: "column",
		name: def.column.column,
		dataType,
		typeInfo,
		isNullable: def.nullable?.type !== "not null",
		defaultValue: def.default_val?.value?.toString(),
	};
}

function parseColumnTypeInfo(
	dataType: string,
	length?: number | string,
): ColumnTypeInfo {
	const typeLower = dataType.toLowerCase();
	if (typeLower === "text") {
		return { kind: "text" };
	}
	if (typeLower === "varchar" || typeLower === "nvarchar") {
		if (length) {
			return { kind: typeLower, length: Number.parseInt(length as string) };
		}
		return { kind: typeLower, length: null };
	}
	return { kind: "simple" };
}

function parseConstraint(
	def: CreateConstraintDefinition,
	schema: string,
	tableName: string,
	rawSql: string,
): Constraint | null {
	if (def.constraint_type === "primary key") {
		return {
			type: "constraint",
			constraintType: "primaryKey",
			name: def.constraint || "",
			columns:
				(def.definition as Array<{ column: string }>)?.map(
					(col) => col.column,
				) || [],
			rawSql,
			clustered: rawSql.includes("CLUSTERED"),
		} as PrimaryKeyConstraint;
	}
	if (def.constraint_type === "FOREIGN KEY") {
		const fkDef = def as CreateConstraintDefinition & {
			definition: Array<{ column: string }>;
			reference_definition: ReferenceDefinition;
		};
		const sourceColumns = fkDef.definition?.map((col) => col.column) || [];
		const referencedColumns =
			fkDef.reference_definition?.columns?.map((col) => col.column) || [];

		if (sourceColumns.length === 0 || referencedColumns.length === 0) {
			console.warn("Invalid foreign key constraint: missing columns");
			return null;
		}

		return {
			type: "constraint",
			constraintType: "foreignKey",
			name: def.constraint || "",
			sourceTable: tableName,
			sourceSchema: schema,
			sourceColumn: sourceColumns[0],
			referencedTable: fkDef.reference_definition?.table?.[0]?.table || "",
			referencedSchema: fkDef.reference_definition?.table?.[0]?.db || "dbo",
			referencedColumn: referencedColumns[0],
			rawSql,
		} as ForeignKeyConstraint;
	}
	if (def.constraint_type === "unique") {
		return {
			type: "constraint",
			constraintType: "unique",
			name: def.constraint || "",
			table: tableName,
			schema,
			columns:
				(def.definition as Array<{ column: string }>)?.map(
					(col) => col.column,
				) || [],
			rawSql,
		} as UniqueConstraint;
	}
	if (def.constraint_type === "check") {
		const checkDef = def as CreateConstraintDefinition & {
			definition: { expr: unknown };
		};
		return {
			type: "constraint",
			constraintType: "check",
			name: def.constraint || "",
			table: tableName,
			schema,
			expression: JSON.stringify(checkDef.definition?.expr) || "",
			rawSql,
		} as CheckConstraint;
	}
	return null;
}

function parseCreateTableManually(stmt: string): Table | null {
	const tableNameMatch = stmt.match(/CREATE TABLE (\[.*?\]\.\[.*?\]|\[.*?\])/i);
	if (!tableNameMatch) {
		return null;
	}
	const tableNameStr = tableNameMatch[1];
	const [schema, tableName] = tableNameStr
		.split(".")
		.map((s) => s.trim().slice(1, -1));

	const lines = stmt.split("\n").map((line) => line.trim());
	const startIndex = lines.findIndex((line) => line.includes("("));
	const endIndex = lines.lastIndexOf(")");
	if (startIndex === -1 || endIndex === -1) {
		return null;
	}
	const definitionLines = lines
		.slice(startIndex + 1, endIndex)
		.filter((line) => line && !line.startsWith("--"));

	const definitions: string[] = [];
	let i = 0;
	while (i < definitionLines.length) {
		const line = definitionLines[i];
		if (line.startsWith("CONSTRAINT")) {
			let constraintDef = line;
			let parenLevel =
				(line.match(/\(/g) || []).length - (line.match(/\)/g) || []).length;
			i++;
			while (i < definitionLines.length && parenLevel > 0) {
				const nextLine = definitionLines[i];
				constraintDef += ` ${nextLine}`;
				parenLevel +=
					(nextLine.match(/\(/g) || []).length -
					(nextLine.match(/\)/g) || []).length;
				i++;
			}
			definitions.push(constraintDef.trim().replace(/,\s*$/, ""));
		} else {
			definitions.push(line.replace(/,\s*$/, ""));
			i++;
		}
	}

	const columns: Column[] = [];
	const constraints: Constraint[] = [];

	for (const def of definitions) {
		if (def.startsWith("CONSTRAINT")) {
			const constraint = parseConstraintLine(def, schema || "dbo", tableName);
			if (constraint) {
				constraints.push(constraint);
			}
		} else {
			const column = parseColumnLine(def);
			if (column) {
				columns.push(column);
			}
		}
	}

	return {
		type: "table",
		name: tableName,
		schema: schema || "dbo",
		comment: "",
		rawSql: stmt,
		columns,
		constraints,
		fileGroup: stmt.match(/ON \[([^\]]+)\]/)?.[1] || undefined,
	};
}

function parseColumnLine(line: string): Column | null {
	const match = line.match(/\[(.+?)\]\s+\[(.+?)\](?:\((.+?)\))?(?:\s+(.+))?/);
	if (!match) {
		return null;
	}
	const name = match[1];
	const type = match[2];
	const lengthStr = match[3];
	const options = match[4] || "";

	let dataType = type;
	if (lengthStr) {
		dataType += `(${lengthStr})`;
	}

	const typeInfo = parseColumnTypeInfo(dataType);
	const isNullable = !options.toUpperCase().includes("NOT NULL");

	const defaultMatch = options.match(/DEFAULT\s+(.+?)(?:\s+|$)/i);
	const defaultValue = defaultMatch ? defaultMatch[1] : undefined;

	return {
		type: "column",
		name,
		dataType,
		typeInfo,
		isNullable,
		defaultValue,
	};
}

function parseConstraintLine(
	line: string,
	schema: string,
	tableName: string,
): Constraint | null {
	const match = line.match(
		/CONSTRAINT \[(.+?)\]\s+(.+?)\s*\((.+?)\)(?:\s+(.+))?/,
	);
	if (!match) {
		return null;
	}
	const name = match[1];
	let typeStr = match[2].toUpperCase();
	const columnsStr = match[3];
	const options = match[4] || "";

	const clustered = typeStr.includes("CLUSTERED");
	const nonClustered = typeStr.includes("NONCLUSTERED");
	if (clustered || nonClustered) {
		typeStr = typeStr
			.replace("CLUSTERED", "")
			.replace("NONCLUSTERED", "")
			.trim();
	}

	const columns = columnsStr.split(",").map((col) =>
		col
			.trim()
			.slice(1, -1)
			.replace(/\s+(ASC|DESC)/gi, ""),
	);

	if (typeStr === "PRIMARY KEY") {
		return {
			type: "constraint",
			constraintType: "primaryKey",
			name,
			columns,
			rawSql: line,
			clustered,
		} as PrimaryKeyConstraint;
	}
	if (typeStr === "UNIQUE") {
		return {
			type: "constraint",
			constraintType: "unique",
			name,
			table: tableName,
			schema,
			columns,
			rawSql: line,
		} as UniqueConstraint;
	}
	if (typeStr === "FOREIGN KEY") {
		const refMatch = options.match(
			/REFERENCES \[(.+?)\](?:\.\[(.+?)\])?\s*\(\[(.+?)\]\)/i,
		);
		if (refMatch) {
			const refSchema = refMatch[1];
			const refTable = refMatch[2] || refSchema;
			const refColumn = refMatch[3];
			return {
				type: "constraint",
				constraintType: "foreignKey",
				name,
				sourceTable: tableName,
				sourceSchema: schema,
				sourceColumn: columns[0],
				referencedTable: refTable,
				referencedSchema: refMatch[2] ? refSchema : "dbo",
				referencedColumn: refColumn,
				rawSql: line,
			} as ForeignKeyConstraint;
		}
	}
	if (typeStr === "CHECK") {
		return {
			type: "constraint",
			constraintType: "check",
			name,
			table: tableName,
			schema,
			expression: columnsStr,
			rawSql: line,
		} as CheckConstraint;
	}
	return null;
}

interface DatabaseStatistics {
	totalTables: number;
	totalColumns: number;
	constraintCounts: {
		total: number;
		primaryKey: number;
		foreignKey: number;
		default: number;
		unique: number;
		check: number;
	};
	failedStatementsCount: number;
}

function calculateStatistics(
	database: Database,
	failedStatements: string[],
): DatabaseStatistics {
	const totalTables = database.tables.length;
	let totalColumns = 0;
	const constraintCounts = {
		total: 0,
		primaryKey: 0,
		foreignKey: 0,
		default: 0,
		unique: 0,
		check: 0,
	};

	for (const table of database.tables) {
		totalColumns += table.columns.length;
		for (const constraint of table.constraints) {
			constraintCounts.total++;
			if (constraint.constraintType === "primaryKey") {
				constraintCounts.primaryKey++;
			} else if (constraint.constraintType === "foreignKey") {
				constraintCounts.foreignKey++;
			} else if (constraint.constraintType === "default") {
				constraintCounts.default++;
			} else if (constraint.constraintType === "unique") {
				constraintCounts.unique++;
			} else if (constraint.constraintType === "check") {
				constraintCounts.check++;
			}
		}
	}

	return {
		totalTables,
		totalColumns,
		constraintCounts,
		failedStatementsCount: failedStatements.length,
	};
}

function printStatistics(stats: DatabaseStatistics): void {
	console.log("\nParsing Statistics:");
	console.log(`Found ${stats.totalTables} tables`);
	console.log(`Found ${stats.totalColumns} columns`);
	console.log(`Found ${stats.constraintCounts.total} constraints total:`);
	console.log(
		`  - ${stats.constraintCounts.primaryKey} primary key constraints`,
	);
	console.log(
		`  - ${stats.constraintCounts.foreignKey} foreign key constraints`,
	);
	console.log(`  - ${stats.constraintCounts.default} default constraints`);
	console.log(`  - ${stats.constraintCounts.unique} unique constraints`);
	console.log(`  - ${stats.constraintCounts.check} check constraints`);
	console.log(`Failed statements: ${stats.failedStatementsCount}`);
}

if (require.main === module) {
	const args = process.argv.slice(2);
	const dumpFailedOnly = args.includes("--dump-failed-only");
	const dumpTableArg = args.find((arg) => arg.startsWith("--dump-table="));
	const dumpTableName = dumpTableArg ? dumpTableArg.split("=")[1] : null;
	const dumpParsedDatabase = args.includes("--dump-parsed-database");
	const inputFile = args.find((arg) => !arg.startsWith("--"));

	if (!inputFile) {
		console.error(
			"Usage: ts-node process.ts <input-sql-file> [--dump-failed-only | --dump-table=<tablename> | --dump-parsed-database]",
		);
		process.exit(1);
	}

	try {
		const sqlContent = fs.readFileSync(path.resolve(inputFile), "utf8");
		const { database, failedStatements } = parseSqlDump(sqlContent);

		if (dumpTableName) {
			// Determine the table key (schema.table)
			let tableKey: string;
			if (dumpTableName.includes(".")) {
				tableKey = dumpTableName;
			} else {
				tableKey = `dbo.${dumpTableName}`; // Default to dbo schema if not specified
			}

			// Find the table in the database
			const table = database.tables.find(
				(t) => `${t.schema}.${t.name}` === tableKey,
			);
			if (table) {
				console.log(`Table: ${tableKey}\n`);
				console.log("CREATE TABLE statement:");
				console.log(table.rawSql);
				console.log("\nALTER TABLE statements:");
				// Filter constraints to get only ALTER TABLE statements (exclude those from CREATE TABLE)
				const alterStatements = table.constraints
					.filter((constraint) => constraint.rawSql !== table.rawSql)
					.map((constraint) => constraint.rawSql);
				if (alterStatements.length > 0) {
					console.log(alterStatements.join("\n\n"));
				} else {
					console.log("No ALTER TABLE statements.");
				}
				console.log("\nParsed Table AST:");
				console.log(JSON.stringify(table, null, 2));
			} else {
				console.log(`Table ${tableKey} not found.`);
			}
		} else if (dumpFailedOnly) {
			console.log("\nStatements that Failed to Generate an AST:");
			for (const stmt of failedStatements) {
				console.log(stmt);
			}
			console.log(`Total failed statements: ${failedStatements.length}`);
		} else if (dumpParsedDatabase) {
			console.log(JSON.stringify({ database }, null, 2));
		} else {
			console.log("\nParsed Database:");
			console.log(JSON.stringify(database, null, 2));
		}

		// Always print statistics
		const stats = calculateStatistics(database, failedStatements);
		printStatistics(stats);
	} catch (error) {
		console.error("Error:", (error as Error).message);
		process.exit(1);
	}
}
