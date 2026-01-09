#!/bin/bash
#
# Setup script for databricks-claude-test-project
# Installs skills and configures the MCP server
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILLS_DIR="../databricks-skills"
MCP_SERVER_DIR="../databricks-mcp-server"
TOOLS_CORE_DIR="../databricks-tools-core"

echo "=========================================="
echo "Setting up Databricks Claude Test Project"
echo "=========================================="
echo ""

# Check for uv
if ! command -v uv &> /dev/null; then
    echo "Error: 'uv' is not installed."
    echo "Install it with: curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi
echo "✓ uv is installed"

# Check if skills directory exists
if [ ! -d "$SCRIPT_DIR/$SKILLS_DIR" ]; then
    echo "Error: Skills directory not found at $SCRIPT_DIR/$SKILLS_DIR"
    echo "Make sure you're running from the ai-dev-kit repository."
    exit 1
fi
echo "✓ Skills directory found"

# Check if MCP server directory exists
if [ ! -d "$SCRIPT_DIR/$MCP_SERVER_DIR" ]; then
    echo "Error: MCP server directory not found at $SCRIPT_DIR/$MCP_SERVER_DIR"
    echo "Make sure you're running from the ai-dev-kit repository."
    exit 1
fi
echo "✓ MCP server directory found"

# Verify MCP server is set up
MCP_SERVER_ABS="$SCRIPT_DIR/$MCP_SERVER_DIR"
MCP_PYTHON="$MCP_SERVER_ABS/.venv/bin/python"

echo ""
echo "Checking MCP server setup..."
if [ ! -f "$MCP_PYTHON" ]; then
    echo "Error: MCP server venv not found at $MCP_SERVER_ABS/.venv"
    echo ""
    echo "Please set up the MCP server first by running:"
    echo "  cd $MCP_SERVER_ABS"
    echo "  ./setup.sh   # or: uv venv && uv pip install -e ../databricks-tools-core -e ."
    exit 1
fi

# Verify the server can be imported
if ! "$MCP_PYTHON" -c "import databricks_mcp_server" 2>/dev/null; then
    echo "Error: MCP server packages not installed properly"
    echo ""
    echo "Please set up the MCP server first by running:"
    echo "  cd $MCP_SERVER_ABS"
    echo "  ./setup.sh   # or: uv pip install -e ../databricks-tools-core -e ."
    exit 1
fi
echo "✓ MCP server is set up"

# Run the install skills script
echo ""
echo "Installing Databricks skills..."
cd "$SCRIPT_DIR"
"$SCRIPT_DIR/$SKILLS_DIR/install_skills.sh" "$@"

# Check for claude CLI
if ! command -v claude &> /dev/null; then
    echo ""
    echo "Warning: 'claude' CLI is not installed."
    echo "Install it from: https://claude.ai/code"
    echo "Skipping MCP server registration."
else
    # Register MCP server with Claude Code
    echo ""
    echo "Registering Databricks MCP server with Claude Code..."
    cd "$SCRIPT_DIR"

    # Remove existing server if present (to update config)
    claude mcp remove databricks 2>/dev/null || true

    # Add the MCP server using claude mcp add-json
    # Use the runner script to avoid Python -m module reimport issues
    MCP_RUNNER="$MCP_SERVER_ABS/run_server.py"
    if [ ! -f "$MCP_RUNNER" ]; then
        echo "Error: MCP runner script not found at $MCP_RUNNER"
        exit 1
    fi
    claude mcp add-json databricks "{\"command\":\"$MCP_PYTHON\",\"args\":[\"$MCP_RUNNER\"]}"
    echo "✓ MCP server registered"
fi

# Create/reset CLAUDE.md
echo ""
echo "Creating CLAUDE.md..."
cat > "$SCRIPT_DIR/CLAUDE.md" << 'EOF'
# Project Context

This is a test project for experimenting with Databricks MCP tools and Claude Code.

## Available Tools

You have access to Databricks MCP tools prefixed with `mcp__databricks__`. Use `/mcp` to see the list of available tools.

## Skills

Load skills for detailed guidance:
- `skill: "sdp"` - Spark Declarative Pipelines
- `skill: "databricks-python-sdk"` - Python SDK patterns
- `skill: "synthetic-data-generation"` - Test data generation
- `skill: "dabs-writer"` - Databricks Asset Bundles

## Testing Workflow

1. Start with simple queries to verify MCP connection works
2. Test individual tools before combining them
3. Use skills when building pipelines or complex workflows

## Notes

This is a sandbox for testing - feel free to create files, run queries, and experiment.
EOF
echo "Created CLAUDE.md"

echo ""
echo "========================================"
echo "Setup complete!"
echo "========================================"
echo ""
echo "Next steps:"
echo "  1. Make sure DATABRICKS_HOST and DATABRICKS_TOKEN are set"
echo "  2. Run: claude"
echo "  3. Test the MCP tools with commands like:"
echo "     - 'List my SQL warehouses'"
echo "     - 'Run a simple SQL query'"
echo ""
