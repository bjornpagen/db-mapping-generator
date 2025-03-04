# Database Mapping and Query Generation Tools

This repository contains a set of command-line interface (CLI) tools designed to facilitate mapping data from a source database (T-SQL) to a target database (MySQL), aligned with the TM Forum SID data model. The tools leverage OpenAI to generate semantic mappings between database schemas and produce SQL queries for data extraction and insertion. This project automates key aspects of database migration, making it easier to transform and transfer data between systems.

## What I've Accomplished

- **AI-Powered Mapping**: Developed a tool (`generate.ts`) that uses OpenAI to intelligently map columns from a T-SQL source database to a MySQL target database based on semantic meaning, reducing manual effort.
- **Query Generation**: Created scripts to generate T-SQL `SELECT` queries (`generate-select-queries.ts`) for extracting data from the source database and MySQL `INSERT` queries (`generate-insert-queries.ts`) for populating the target database, complete with dependency handling.
- **Robust Utilities**: Built reusable utilities for error handling (`errors.ts`) and rate limiting (`rate-limiter.ts`) to ensure reliable operation, especially when interacting with the OpenAI API.
- **Modular Design**: Structured the codebase with clear interfaces (`mapping.ts`, `relational.ts`) and modular scripts, making it easy to extend or maintain.

## Features

- **Mapping Generation**: Automatically generates mappings between source and target database columns using AI, considering column names, descriptions, and TM Forum SID standards.
- **SELECT Query Generation**: Produces T-SQL `SELECT` queries to extract data from the source database based on the generated mappings.
- **INSERT Query Generation**: Generates MySQL `INSERT` queries with placeholders, handling primary/foreign key constraints and dependency cycles for correct data insertion order.

## Prerequisites

Before using these tools, ensure you have the following:

- **Node.js and Bun**: Installed on your system (Bun is used as the runtime).
- **OpenAI API Key**: Set as an environment variable (`OPENAI_API_KEY`) for AI-powered mapping generation.
- **Database Schema Files**: JSON files representing the source (T-SQL) and target (MySQL) database schemas, placed in the `dumps/` directory (e.g., `dumps/logisense.tsql.json` and `dumps/sid.mysql.json`).

## Installation

1. **Clone the Repository**:
   ```sh
   git clone <repository-url>
   cd <repository-name>
   ```

2. **Install Dependencies**:
   ```sh
   bun install
   ```

3. **Set Environment Variable**:
   ```sh
   export OPENAI_API_KEY=<your-openai-api-key>
   ```
   Alternatively, add it to a `.env` file in the root directory:
   ```
   OPENAI_API_KEY=<your-openai-api-key>
   ```

## Usage

The repository includes three primary CLI tools, each with a specific role in the data mapping and migration process. Below are the commands and instructions for using them.

### 1. Generate Mapping (`generate.ts`)

**Purpose**: Creates a mapping between source and target database schemas using OpenAI.

**Command**:
```sh
bun run generate.ts > mapping.json
```

**Details**:
- **Input**: Reads schema files from hardcoded paths:
  - Source: `dumps/logisense.tsql.json`
  - Target: `dumps/sid.mysql.json`
- **Configuration**: Uses a predefined list of source tables in `generate.ts` (e.g., `dbo.User`, `dbo.Contact`). Edit the `config` object in the script to customize this list.
- **Output**: Prints a JSON mapping to the console, which you should redirect to a file (e.g., `mapping.json`) for use with other tools.

### 2. Generate SELECT Queries (`generate-select-queries.ts`)

**Purpose**: Generates T-SQL `SELECT` queries to extract data from the source database based on the mapping.

**Command**:
```sh
bun run generate-select-queries.ts <mapping-file-path> > select_queries.sql
```

**Example**:
```sh
bun run generate-select-queries.ts mapping.json > select_queries.sql
```

**Details**:
- **Input**: Path to the mapping JSON file (e.g., `mapping.json`).
- **Output**: Outputs `SELECT` queries to the console, one per source table in the mapping. Redirect to a file for later use.

### 3. Generate INSERT Queries (`generate-insert-queries.ts`)

**Purpose**: Generates MySQL `INSERT` queries for the target database, using placeholders for values and handling dependencies.

**Command**:
```sh
bun run generate-insert-queries.ts <mapping-file-path> <source-table1> <source-table2> ...
```

**Example**:
```sh
bun run generate-insert-queries.ts mapping.json dbo.User dbo.Contact
```

**Details**:
- **Input**: Mapping JSON file path and a list of source tables (e.g., `dbo.User`).
- **Output**: Writes `INSERT` queries to `insert_queries.sql` in the current directory. Includes placeholders (e.g., `:dbo.User.ID`) and handles dependency cycles with `UPDATE` statements.

## How to Use the Tools Together

Here’s a step-by-step workflow to map and migrate data using these tools:

1. **Generate the Mapping**:
   ```sh
   bun run generate.ts > mapping.json
   ```
   - Creates a `mapping.json` file with column mappings.

2. **Generate SELECT Queries**:
   ```sh
   bun run generate-select-queries.ts mapping.json > select_queries.sql
   ```
   - Produces `select_queries.sql` with T-SQL queries for data extraction.

3. **Extract Data**:
   - Manually execute `select_queries.sql` on the source T-SQL database using a database client (e.g., SQL Server Management Studio) to retrieve the data.

4. **Generate INSERT Queries**:
   ```sh
   bun run generate-insert-queries.ts mapping.json dbo.User dbo.Contact
   ```
   - Creates `insert_queries.sql` with MySQL `INSERT` statements for the specified source tables and their dependencies.

5. **Prepare INSERT Queries**:
   - Replace placeholders in `insert_queries.sql` (e.g., `:dbo.User.ID`) with actual data from the `SELECT` query results. This step requires manual scripting or a separate tool.

6. **Insert Data**:
   - Execute the modified `insert_queries.sql` on the target MySQL database using a client (e.g., MySQL Workbench) to populate the data.

**Note**: Steps 3, 5, and 6 are not automated by these scripts and require manual intervention or additional tooling to complete the migration.

## Example Workflow

Suppose you want to migrate `dbo.User` and `dbo.Contact` tables:

1. **Generate Mapping**:
   ```sh
   bun run generate.ts > mapping.json
   ```

2. **Generate SELECT Queries**:
   ```sh
   bun run generate-select-queries.ts mapping.json > select_queries.sql
   ```

3. **Generate INSERT Queries**:
   ```sh
   bun run generate-insert-queries.ts mapping.json dbo.User dbo.Contact
   ```

4. **Next Steps**:
   - Run `select_queries.sql` on the source database.
   - Replace placeholders in `insert_queries.sql` with extracted data.
   - Execute `insert_queries.sql` on the target database.

## Additional Utilities

- **`errors.ts`**:
  - Provides structured error handling with `tryCatch` (async), `trySync` (sync), and `WrappedError` for wrapping errors with context.
  - Used throughout the scripts for robust error management.

- **`rate-limiter.ts`**:
  - Implements `retryWithExponentialBackoff` to handle OpenAI API rate limits with exponential backoff retries.

## Dependencies

- **OpenAI API**: Requires an API key (`OPENAI_API_KEY`). Usage may incur costs based on request volume and token usage.

## Important Notes

- **Placeholders**: `INSERT` queries use placeholders (e.g., `:schema.table.column`). You must replace these with actual values manually or via a script.
- **Schema Files**: Ensure JSON schema files are correctly formatted with tables, columns, and constraints.
- **AI Dependency**: Mapping quality depends on OpenAI’s interpretation of column names and descriptions.
- **Manual Steps**: Data extraction and insertion require external tools or manual execution, as these scripts focus on query generation.
