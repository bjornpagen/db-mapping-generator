# DB Mapping Generator

A proof-of-concept tool that uses AI to generate mappings between database schemas for telecom migrations, targeting the TMForum SID model.

## What It Does

This tool takes a source database schema (e.g., T-SQL) and maps it to a MySQL target schema aligned with TMForum SID. It leverages OpenAI to analyze schemas semantically, producing a `Mapping` object that can be lowered into executable SQL. It’s built to handle telecom data migrations with complex relationships, reducing manual effort and errors.

## Tech Stack

- **Bun**: A fast JavaScript runtime (like Node, but quicker). Install with `curl -fsSL https://bun.sh/install | bash`.
- **TypeScript**: Strongly-typed logic in `generate.ts`.
- **OpenAI**: Drives the semantic mapping (uses `o3-mini` by default).

## Installation

```bash
git clone https://github.com/yourusername/db-mapping-generator.git
cd db-mapping-generator
bun install
echo "OPENAI_API_KEY=your-api-key-here" > .env
```

## Input

### Source Schema
- **Format**: JSON as a `Database` type (see `relational.ts`).
- **How It’s Made**: Parsed from a T-SQL connection string by querying the information schema (`INFORMATION_SCHEMA.TABLES`, `.COLUMNS`, `.KEY_COLUMN_USAGE`, etc.). Captures tables, columns, constraints, and relationships.
- **Example**: `logisense.tsql.json` - a legacy telecom billing schema.

### Target Schema
- **Format**: JSON as a `Database` type, representing a MySQL DB.
- **How It’s Made**: Derived from a MySQL connection string, structured to match SID entities (e.g., `party`, `party_role`, `product_offering`).
- **Example**: `sid.mysql.json` - a SID-compliant schema with proper relationships.

## How It Works

1. **Schema Parsing**
   - Reads connection strings, queries the information schema, and builds `Database` objects for source and target.
   - Source: T-SQL schema → `Database` with tables/columns/constraints.
   - Target: MySQL SID schema → `Database` with SID-aligned structure.

2. **Mapping Generation**
   - **AI Step**: OpenAI analyzes both schemas, mapping columns semantically (not just by name).
   - **Algorithm**:
     - Builds dependency graphs with Tarjan’s SCC to handle cyclic relationships.
     - Constructs prompts with source/target details and SID context.
     - Parses AI responses into a `Mapping` (see `mapping.ts`).
   - **Output**: A JSON `Mapping` with `columnMappings`:
     ```json
     {
       "type": "mapping",
       "in": { "tsql_0": {/*source db*/} },
       "out": {/*target db*/},
       "columnMappings": [
         {
           "sources": [{"databaseId": "tsql_0", "schema": "dbo", "table": "Customer", "column": "Name"}],
           "destinationSchema": "mysql",
           "destinationTable": "party_name",
           "destinationColumn": "name",
           "description": "Direct mapping from 'Name' to 'name'."
         }
       ]
     }
     ```

3. **SQL Lowering (WIP)**
   - The `Mapping` includes all relationship info (FKs, PKs, transformations).
   - Next step: Lower it into SQL `INSERT`/`SELECT` statements with joins, executable against both DBs.

## Usage

```bash
bun run generate.ts > mapping-output.json
```

- **Defaults**: Maps `logisense.tsql.json` to `sid.mysql.json`.
- **Customize**: Edit `generate.ts` (`IN_DATABASE_PATH`, `OUT_DATABASE_PATH`, `targetTables`).

## For SID Experts

- Maps to SID entities (e.g., `party`, `party_role_association`) with semantic fidelity.
- Respects referential integrity and SID conventions.
- AI handles transformations (e.g., splitting `full_name` into `first_name`/`last_name`).

## Next Steps

- Finish SQL lowering for execution.
- Add validation for data integrity.
- Test with real telecom datasets.

This is a demo to show AI can tackle telecom migrations efficiently. Thoughts?
