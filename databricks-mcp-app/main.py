#!/usr/bin/env python
"""
Databricks MCP App - Main entry point

Serves the ai-dev-kit MCP server over HTTP/SSE transport for use with
Databricks managed MCP clients and AI agents.
"""

import os
import sys
import logging

# Add the app directory to sys.path for local imports
app_dir = os.path.dirname(os.path.abspath(__file__))
if app_dir not in sys.path:
    sys.path.insert(0, app_dir)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Start the MCP server with streamable-http transport."""
    # Databricks apps default to port 8000
    port = int(os.getenv("PORT", "8000"))
    
    logger.info(f"Starting Databricks MCP Server on port {port}")
    logger.info("Transport: streamable-http")
    
    try:
        # Import the MCP server from databricks-mcp-server
        from databricks_mcp_server.server import mcp
        
        logger.info("MCP Server initialized with tools:")
        logger.info("  - sql, compute, jobs, pipelines")
        logger.info("  - aibi_dashboards, agent_bricks, genie")
        logger.info("  - file, volume_files, unity_catalog, serving")
        logger.info("  - skills: Access Databricks skill documentation")
        
        # Run with streamable-http transport
        # This exposes the MCP server at /mcp endpoint
        mcp.run(
            transport="streamable-http",
            host="0.0.0.0",
            port=port,
            path="/mcp"
        )
        
    except ImportError as e:
        logger.error(f"Failed to import MCP server: {e}")
        logger.error("Ensure databricks-mcp-server and databricks-tools-core are installed")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to start MCP server: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
