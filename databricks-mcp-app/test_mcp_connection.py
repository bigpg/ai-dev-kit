# Databricks notebook source
# MAGIC %md
# MAGIC # Test AI Dev Kit MCP Server Connection
# MAGIC 
# MAGIC This notebook tests the connection to the custom MCP server deployed as a Databricks App.

# COMMAND ----------

# MAGIC %pip install databricks-mcp --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MCP Server URL - update if your app has a different name
MCP_SERVER_URL = "https://ai-dev-kit-mcp-3438839487639471.11.azure.databricksapps.com/mcp"

print(f"Testing MCP Server at: {MCP_SERVER_URL}")

# COMMAND ----------

from databricks_mcp import DatabricksMCPClient
from databricks.sdk import WorkspaceClient

# Use notebook's auth (runs as the current user)
workspace_client = WorkspaceClient()

# Create MCP client
mcp_client = DatabricksMCPClient(
    server_url=MCP_SERVER_URL, 
    workspace_client=workspace_client
)

print("MCP Client created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## List Available Tools

# COMMAND ----------

# List all tools
tools = mcp_client.list_tools()

print(f"Found {len(tools)} tools:\n")
for i, tool in enumerate(tools):
    print(f"{i+1}. {tool.name}")
    if tool.description:
        print(f"   {tool.description[:80]}...")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Calling a Tool

# COMMAND ----------

# Test the list_skills tool
print("Calling list_skills tool...")
result = mcp_client.call_tool("list_skills", {})
print(f"\nResult:\n{result}")

# COMMAND ----------

# Test execute_sql tool (if you have a warehouse)
print("Calling execute_sql tool...")
result = mcp_client.call_tool("execute_sql", {
    "sql": "SELECT current_catalog(), current_schema()"
})
print(f"\nResult:\n{result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use MCP Tools in an Agent
# MAGIC 
# MAGIC Once connected, you can use these tools with LangGraph or OpenAI agents.
# MAGIC See the Databricks documentation for examples:
# MAGIC - [LangGraph MCP Agent](https://docs.databricks.com/notebooks/source/generative-ai/langgraph-mcp-tool-calling-agent.html)
# MAGIC - [OpenAI MCP Agent](https://docs.databricks.com/notebooks/source/generative-ai/openai-mcp-tool-calling-agent.html)
