#!/usr/bin/env python
"""
AI Dev Kit MCP Server - Main entry point

Serves the ai-dev-kit MCP server over HTTP/SSE transport for use with
Databricks managed MCP clients and AI agents.

Includes health check endpoints for browser testing.
"""

import os
import sys
import json
import logging

# Add parent directory to sys.path for local package imports
app_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if app_dir not in sys.path:
    sys.path.insert(0, app_dir)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Start the MCP server with health endpoints."""
    import uvicorn
    from starlette.responses import JSONResponse, HTMLResponse
    
    port = int(os.getenv("PORT", "8000"))
    
    logger.info(f"Starting AI Dev Kit MCP Server on port {port}")
    
    try:
        # Import the MCP server
        from databricks_mcp_server.server import mcp
        
        # Get tool list for health endpoint
        tools = list(mcp._tool_manager._tools.keys())
        
        # Get skills list
        skills = []
        try:
            from databricks_mcp_server.tools.skills import _get_skills_dir
            import pathlib
            skills_path = _get_skills_dir()
            logger.info(f"Skills directory: {skills_path}")
            logger.info(f"Skills dir exists: {skills_path.exists()}")
            
            if skills_path.exists():
                for skill_dir in sorted(skills_path.iterdir()):
                    if skill_dir.is_dir() and not skill_dir.name.startswith('.') and skill_dir.name != 'TEMPLATE':
                        skill_file = skill_dir / "SKILL.md"
                        if skill_file.exists():
                            # Parse frontmatter for description
                            content = skill_file.read_text()
                            description = ""
                            if content.startswith('---'):
                                parts = content.split('---', 2)
                                if len(parts) >= 3:
                                    for line in parts[1].strip().split('\n'):
                                        if line.startswith('description:'):
                                            description = line.split(':', 1)[1].strip().strip('"\'')
                                            break
                            skills.append({
                                "name": skill_dir.name,
                                "description": description[:100] if description else "No description"
                            })
            logger.info(f"Loaded {len(skills)} skills")
        except Exception as e:
            logger.warning(f"Could not load skills: {e}")
            import traceback
            traceback.print_exc()
        
        # Build home page HTML
        skills_html = ""
        for s in skills:
            skills_html += f'<div class="skill"><code>{s["name"]}</code> - {s["description"]}</div>'
        
        home_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>AI Dev Kit MCP Server</title>
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
                       max-width: 900px; margin: 50px auto; padding: 20px; }}
                h1 {{ color: #1b3a57; }}
                h2 {{ color: #2c5282; margin-top: 30px; }}
                .endpoint {{ background: #f5f5f5; padding: 10px; margin: 10px 0; border-radius: 5px; }}
                code {{ background: #e0e0e0; padding: 2px 6px; border-radius: 3px; font-size: 13px; }}
                .section {{ max-height: 350px; overflow-y: auto; border: 1px solid #ddd; 
                           border-radius: 5px; padding: 10px; margin: 10px 0; }}
                .tool, .skill {{ padding: 8px 5px; border-bottom: 1px solid #eee; }}
                .tool:last-child, .skill:last-child {{ border-bottom: none; }}
                .columns {{ display: flex; gap: 20px; }}
                .column {{ flex: 1; }}
                pre {{ background: #f8f8f8; padding: 15px; border-radius: 5px; overflow-x: auto; }}
                .stats {{ display: flex; gap: 20px; margin: 20px 0; }}
                .stat {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                        color: white; padding: 20px; border-radius: 10px; text-align: center; flex: 1; }}
                .stat-number {{ font-size: 36px; font-weight: bold; }}
                .stat-label {{ font-size: 14px; opacity: 0.9; }}
            </style>
        </head>
        <body>
            <h1>AI Dev Kit MCP Server</h1>
            <p>Model Context Protocol server providing tools and skills for Databricks development.</p>
            
            <div class="stats">
                <div class="stat">
                    <div class="stat-number">{len(tools)}</div>
                    <div class="stat-label">Tools</div>
                </div>
                <div class="stat">
                    <div class="stat-number">{len(skills)}</div>
                    <div class="stat-label">Skills</div>
                </div>
            </div>
            
            <h2>Endpoints</h2>
            <div class="endpoint"><strong>GET /</strong> - This page</div>
            <div class="endpoint"><strong>GET /health</strong> - Health check (JSON)</div>
            <div class="endpoint"><strong>GET /tools</strong> - List all tools (JSON)</div>
            <div class="endpoint"><strong>GET /skills</strong> - List all skills (JSON)</div>
            <div class="endpoint"><strong>POST /mcp</strong> - MCP protocol endpoint (for MCP clients)</div>
            
            <div class="columns">
                <div class="column">
                    <h2>Available Tools ({len(tools)})</h2>
                    <div class="section">
                        {"".join(f'<div class="tool"><code>{name}</code></div>' for name in sorted(tools))}
                    </div>
                </div>
                <div class="column">
                    <h2>Available Skills ({len(skills)})</h2>
                    <div class="section">
                        {skills_html if skills_html else '<div class="skill">No skills loaded</div>'}
                    </div>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Add custom routes to the MCP server using fastmcp's custom_route decorator
        @mcp.custom_route("/", methods=["GET"])
        async def home_route(request):
            return HTMLResponse(home_html)
        
        @mcp.custom_route("/health", methods=["GET"])
        async def health_route(request):
            return JSONResponse({
                "status": "healthy",
                "server": "ai-dev-kit-mcp",
                "tools_count": len(tools),
                "skills_count": len(skills),
                "mcp_endpoint": "/mcp"
            })
        
        @mcp.custom_route("/tools", methods=["GET"])
        async def tools_route(request):
            tool_list = []
            for name, tool in mcp._tool_manager._tools.items():
                tool_list.append({
                    "name": name,
                    "description": tool.description[:100] if tool.description else ""
                })
            return JSONResponse({"tools": tool_list, "count": len(tool_list)})
        
        @mcp.custom_route("/skills", methods=["GET"])
        async def skills_route(request):
            return JSONResponse({"skills": skills, "count": len(skills)})
        
        # Create the HTTP app with MCP at /mcp (default)
        app = mcp.http_app()
        
        logger.info(f"MCP Server initialized with {len(tools)} tools")
        logger.info("Endpoints: / (info), /health, /tools, /skills, /mcp (MCP protocol)")
        
        uvicorn.run(app, host="0.0.0.0", port=port)
        
    except ImportError as e:
        logger.error(f"Failed to import: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to start server: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
