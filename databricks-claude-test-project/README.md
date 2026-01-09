# Databricks Claude Test Project

A blank project for testing Claude Code with Databricks MCP tools. Use this to experiment with the MCP server integration and test tools before using them in a real project.

## Prerequisites

- [uv](https://github.com/astral-sh/uv) - Python package manager
- [Claude Code](https://claude.ai/code) - CLI tool

## Quick Start

### 1. Setup

```bash
# Make scripts executable
chmod +x setup.sh cleanup.sh

# Install dependencies, skills, and configure MCP server
./setup.sh
```

This will:
- Check for `uv` and `claude` CLI installation
- Install dependencies for `databricks-tools-core` and `databricks-mcp-server`
- Install Databricks skills to `.claude/skills/`
- Register the Databricks MCP server with Claude Code (using `claude mcp add`)
- Create `CLAUDE.md` with project context

### 2. Configure Databricks Credentials

Set your Databricks credentials:

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
```

Or create a `.env.local` file (gitignored):

```bash
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi...
```

### 3. Run Claude Code

```bash
# Start Claude Code in this directory
claude
```

### 4. Test MCP Tools

Try these commands to test the Databricks MCP integration:

```
# List available warehouses
List my SQL warehouses

# Run a simple query
Run this SQL query: SELECT current_timestamp()

# Check clusters
What clusters do I have available?

# Test Unity Catalog
List the catalogs in my workspace
```

## MCP Server Configuration

The setup script registers the MCP server using `claude mcp add`:

```bash
claude mcp add --transport stdio databricks -- uv run --directory ../databricks-mcp-server python -m databricks_mcp_server.server
```

This registers the `databricks-mcp-server` to run from the sibling directory.

### Manual MCP Configuration

To manually add or reconfigure the MCP server:

```bash
# Remove existing (if any)
claude mcp remove databricks

# Add with custom path
claude mcp add --transport stdio databricks -- uv run --directory /path/to/databricks-mcp-server python -m databricks_mcp_server.server
```

To verify the server is configured:

```bash
claude mcp list
```

## Available MCP Tools

Once configured, Claude has access to these Databricks tools:

| Tool | Description |
|------|-------------|
| `mcp__databricks__execute_sql` | Run SQL queries on a warehouse |
| `mcp__databricks__execute_sql_multi` | Run multiple SQL statements with parallelism |
| `mcp__databricks__list_warehouses` | List available SQL warehouses |
| `mcp__databricks__get_best_warehouse` | Auto-select the best warehouse |
| `mcp__databricks__get_table_details` | Get table schema and statistics |
| `mcp__databricks__execute_databricks_command` | Run code on a cluster |
| `mcp__databricks__run_python_file_on_databricks` | Execute Python files on cluster |
| `mcp__databricks__upload_folder` | Upload folders to workspace |
| `mcp__databricks__upload_file` | Upload files to workspace |
| `mcp__databricks__create_or_update_pipeline` | Create/update SDP pipelines |
| `mcp__databricks__start_update` | Start a pipeline run |
| `mcp__databricks__get_update` | Check pipeline run status |
| `mcp__databricks__get_pipeline_events` | Get pipeline error details |
| `mcp__databricks__stop_pipeline` | Stop a running pipeline |

## Skills

The setup script installs these skills to `.claude/skills/`:

- **sdp** - Spark Declarative Pipelines (SDP/DLT)
- **dabs-writer** - Databricks Asset Bundles
- **databricks-python-sdk** - Python SDK patterns
- **synthetic-data-generation** - Test data generation

Use skills by asking Claude:
```
Load the sdp skill and help me create a pipeline
```

## Cleanup

To reset the project and start fresh:

```bash
./cleanup.sh
```

This removes:
- `.claude/` directory (skills, mcp.json, sessions)
- Generated test files (*.parquet, *.csv, etc.)
- Temporary directories

## Troubleshooting

### MCP Server Not Found

Make sure you're in the `ai-dev-kit` repository and the `databricks-mcp-server` directory exists:

```bash
ls ../databricks-mcp-server/
```

### Authentication Errors

Verify your credentials:

```bash
echo $DATABRICKS_HOST
echo $DATABRICKS_TOKEN
```

### Tool Errors

Check the MCP server logs - Claude Code shows tool errors in the chat. Common issues:
- Invalid warehouse ID
- Missing permissions
- Network connectivity

## Project Structure

```
databricks-claude-test-project/
├── .claude/
│   ├── mcp.json           # MCP server configuration
│   └── skills/            # Installed Databricks skills
│       ├── sdp/
│       ├── dabs-writer/
│       └── ...
├── .gitignore             # Ignores test artifacts
├── CLAUDE.md              # Project context for Claude
├── setup.sh               # Setup script
├── cleanup.sh             # Cleanup script
└── README.md              # This file
```
