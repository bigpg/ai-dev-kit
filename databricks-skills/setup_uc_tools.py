# Databricks notebook source
# MAGIC %md
# MAGIC # Setup UC Tools for Databricks Assistant Agent Mode
# MAGIC 
# MAGIC This notebook creates Unity Catalog functions that serve as tools for the
# MAGIC Databricks Assistant in Agent mode. These functions complement the skills
# MAGIC by providing executable capabilities.
# MAGIC
# MAGIC **Run this notebook once to set up all tools.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Update these for your environment
CATALOG = "main"
SCHEMA = "ai_dev_kit_tools"

print(f"Setting up tools in: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# Create schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} COMMENT 'AI Dev Kit tools for Databricks Assistant'")
print(f"✓ Schema {CATALOG}.{SCHEMA} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Tools

# COMMAND ----------

# Tool: Execute SQL Query
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.execute_sql(query STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Execute a SQL query and return results as JSON. Use for data exploration, creating tables, running analytics, or testing queries before using in dashboards.'
AS $$
    import json
    try:
        result = spark.sql(query)
        rows = result.limit(100).collect()
        columns = result.columns
        data = [dict(zip(columns, [str(v) if v is not None else None for v in row])) for row in rows]
        return json.dumps({{"success": True, "columns": columns, "data": data, "row_count": len(data)}})
    except Exception as e:
        return json.dumps({{"success": False, "error": str(e)}})
$$
""")
print("✓ execute_sql")

# COMMAND ----------

# Tool: Get Table Details
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_table_details(
    catalog_name STRING,
    schema_name STRING
)
RETURNS STRING
LANGUAGE PYTHON  
COMMENT 'Get details of all tables in a schema including columns and types. Essential before writing SQL or creating dashboards.'
AS $$
    import json
    try:
        tables = spark.sql(f"SHOW TABLES IN {{catalog_name}}.{{schema_name}}").collect()
        result = []
        for t in tables[:20]:
            try:
                cols = spark.sql(f"DESCRIBE TABLE {{catalog_name}}.{{schema_name}}.{{t.tableName}}").collect()
                columns = [{{
                    "name": c.col_name, 
                    "type": c.data_type
                }} for c in cols if c.col_name and not c.col_name.startswith('#')]
                result.append({{
                    "table": t.tableName,
                    "columns": columns
                }})
            except:
                result.append({{"table": t.tableName, "columns": []}})
        return json.dumps({{"catalog": catalog_name, "schema": schema_name, "tables": result}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ get_table_details")

# COMMAND ----------

# Tool: List Warehouses
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.list_warehouses()
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'List all SQL warehouses in the workspace with their status. Use to find a warehouse_id for queries or dashboards.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        warehouses = list(w.warehouses.list())
        result = [{{
            "id": wh.id,
            "name": wh.name,
            "state": str(wh.state.value) if wh.state else "UNKNOWN",
            "size": wh.cluster_size
        }} for wh in warehouses]
        return json.dumps({{"warehouses": result}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ list_warehouses")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard Tools

# COMMAND ----------

# Tool: Create Dashboard
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.create_dashboard(
    display_name STRING,
    parent_path STRING,
    dashboard_json STRING,
    warehouse_id STRING
)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Create or update an AI/BI dashboard. IMPORTANT: Test all SQL queries with execute_sql first! Use get_skill(aibi-dashboards) for the correct JSON format.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.dashboards import Dashboard
    try:
        w = WorkspaceClient()
        
        # Check if dashboard exists
        existing = None
        try:
            dashboards = list(w.lakeview.list())
            for d in dashboards:
                if d.display_name == display_name and d.parent_path == parent_path:
                    existing = d
                    break
        except:
            pass
        
        if existing:
            result = w.lakeview.update(
                dashboard_id=existing.dashboard_id,
                display_name=display_name,
                serialized_dashboard=dashboard_json,
                warehouse_id=warehouse_id
            )
            action = "updated"
        else:
            result = w.lakeview.create(
                display_name=display_name,
                parent_path=parent_path,
                serialized_dashboard=dashboard_json,
                warehouse_id=warehouse_id
            )
            action = "created"
        
        # Publish
        try:
            w.lakeview.publish(result.dashboard_id, warehouse_id=warehouse_id)
        except:
            pass
            
        return json.dumps({{
            "success": True,
            "action": action,
            "dashboard_id": result.dashboard_id,
            "path": result.path
        }})
    except Exception as e:
        return json.dumps({{"success": False, "error": str(e)}})
$$
""")
print("✓ create_dashboard")

# COMMAND ----------

# Tool: List Dashboards
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.list_dashboards()
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'List all AI/BI dashboards in the workspace.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        dashboards = list(w.lakeview.list())
        result = [{{
            "id": d.dashboard_id,
            "name": d.display_name,
            "path": d.path
        }} for d in dashboards[:50]]
        return json.dumps({{"dashboards": result, "count": len(result)}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ list_dashboards")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Jobs Tools

# COMMAND ----------

# Tool: Create Job
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.create_job(
    name STRING,
    notebook_path STRING,
    schedule_cron STRING DEFAULT NULL,
    schedule_timezone STRING DEFAULT 'UTC'
)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Create a Databricks job to run a notebook. Optionally add a cron schedule. Use get_skill(databricks-jobs) for advanced configurations.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.jobs import Task, NotebookTask, Source, CronSchedule
    try:
        w = WorkspaceClient()
        
        tasks = [Task(
            task_key="main",
            notebook_task=NotebookTask(
                notebook_path=notebook_path,
                source=Source.WORKSPACE
            )
        )]
        
        schedule = None
        if schedule_cron:
            schedule = CronSchedule(
                quartz_cron_expression=schedule_cron,
                timezone_id=schedule_timezone
            )
        
        job = w.jobs.create(
            name=name,
            tasks=tasks,
            schedule=schedule
        )
        
        return json.dumps({{
            "success": True,
            "job_id": job.job_id,
            "name": name
        }})
    except Exception as e:
        return json.dumps({{"success": False, "error": str(e)}})
$$
""")
print("✓ create_job")

# COMMAND ----------

# Tool: List Jobs  
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.list_jobs()
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'List all jobs in the workspace.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        jobs = list(w.jobs.list(limit=50))
        result = [{{
            "job_id": j.job_id,
            "name": j.settings.name if j.settings else "Unknown"
        }} for j in jobs]
        return json.dumps({{"jobs": result, "count": len(result)}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ list_jobs")

# COMMAND ----------

# Tool: Run Job
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.run_job(job_id BIGINT)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Trigger a job run immediately. Returns the run_id.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        run = w.jobs.run_now(job_id=job_id)
        return json.dumps({{
            "success": True,
            "run_id": run.run_id,
            "job_id": job_id
        }})
    except Exception as e:
        return json.dumps({{"success": False, "error": str(e)}})
$$
""")
print("✓ run_job")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Tools

# COMMAND ----------

# Tool: Create Pipeline
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.create_pipeline(
    name STRING,
    target_catalog STRING,
    target_schema STRING,
    notebook_path STRING
)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Create a Spark Declarative Pipeline (DLT/SDP). Use get_skill(spark-declarative-pipelines) for pipeline code patterns.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        
        pipeline = w.pipelines.create(
            name=name,
            catalog=target_catalog,
            target=target_schema,
            libraries=[{{"notebook": {{"path": notebook_path}}}}],
            continuous=False,
            development=True
        )
        
        return json.dumps({{
            "success": True,
            "pipeline_id": pipeline.pipeline_id,
            "name": name
        }})
    except Exception as e:
        return json.dumps({{"success": False, "error": str(e)}})
$$
""")
print("✓ create_pipeline")

# COMMAND ----------

# Tool: List Pipelines
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.list_pipelines()
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'List all pipelines in the workspace.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        pipelines = list(w.pipelines.list_pipelines())
        result = [{{
            "pipeline_id": p.pipeline_id,
            "name": p.name,
            "state": str(p.state) if p.state else "UNKNOWN"
        }} for p in pipelines[:50]]
        return json.dumps({{"pipelines": result, "count": len(result)}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ list_pipelines")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Skills Tools

# COMMAND ----------

# Tool: List Skills
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.list_skills()
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'List all available AI Dev Kit skills. Skills provide guidance for Databricks tasks like dashboards, jobs, pipelines, etc.'
AS $$
    import json
    skills = [
        {{"name": "aibi-dashboards", "description": "Create AI/BI dashboards with correct JSON format"}},
        {{"name": "databricks-jobs", "description": "Create and manage Databricks Jobs"}},
        {{"name": "spark-declarative-pipelines", "description": "Build DLT/SDP streaming pipelines"}},
        {{"name": "synthetic-data-generation", "description": "Generate test data with Faker"}},
        {{"name": "mlflow-evaluation", "description": "Evaluate ML models and agents"}},
        {{"name": "asset-bundles", "description": "Deploy with Databricks Asset Bundles"}},
        {{"name": "agent-bricks", "description": "Build Knowledge Assistants and Genie Spaces"}},
        {{"name": "databricks-python-sdk", "description": "SDK usage patterns"}},
        {{"name": "model-serving", "description": "Deploy Model Serving endpoints"}},
        {{"name": "databricks-unity-catalog", "description": "Unity Catalog management"}},
        {{"name": "databricks-genie", "description": "Genie Space interactions"}},
        {{"name": "lakebase-provisioned", "description": "Managed PostgreSQL patterns"}},
        {{"name": "databricks-app-python", "description": "Build Python apps (Streamlit/Dash)"}},
        {{"name": "databricks-app-apx", "description": "Build full-stack apps (FastAPI+React)"}},
        {{"name": "databricks-config", "description": "Authentication and configuration"}},
        {{"name": "unstructured-pdf-generation", "description": "Generate PDFs for RAG testing"}}
    ]
    return json.dumps({{"skills": skills, "count": len(skills)}})
$$
""")
print("✓ list_skills")

# COMMAND ----------

# Tool: Get Skill
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_skill(skill_name STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Get detailed documentation for a skill. Use this before implementing Databricks features to get the correct patterns and formats.'
AS $$
    import json
    import os
    
    user = spark.sql("SELECT current_user()").collect()[0][0]
    skill_path = f"/Workspace/Users/{{user}}/.assistant/skills/{{skill_name}}/SKILL.md"
    
    try:
        with open(skill_path, 'r') as f:
            content = f.read()
        return json.dumps({{"skill": skill_name, "content": content}})
    except FileNotFoundError:
        return json.dumps({{"error": f"Skill '{{skill_name}}' not found. Use list_skills() to see available skills."}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ get_skill")

# COMMAND ----------

# Tool: Search Skills
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.search_skills(query STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Search across all skills for relevant guidance. Use when unsure which skill applies to your task.'
AS $$
    import json
    import os
    
    user = spark.sql("SELECT current_user()").collect()[0][0]
    skills_base = f"/Workspace/Users/{{user}}/.assistant/skills"
    query_lower = query.lower()
    matches = []
    
    try:
        for skill_name in os.listdir(skills_base):
            skill_dir = os.path.join(skills_base, skill_name)
            if os.path.isdir(skill_dir):
                skill_file = os.path.join(skill_dir, "SKILL.md")
                if os.path.exists(skill_file):
                    with open(skill_file, 'r') as f:
                        content = f.read()
                    if query_lower in content.lower():
                        # Find context
                        idx = content.lower().find(query_lower)
                        start = max(0, idx - 50)
                        end = min(len(content), idx + len(query) + 50)
                        context = content[start:end].replace('\\n', ' ')
                        matches.append({{
                            "skill": skill_name,
                            "context": f"...{{context}}..."
                        }})
        return json.dumps({{"query": query, "matches": matches[:10]}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ search_skills")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workspace Tools

# COMMAND ----------

# Tool: Upload Notebook
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.create_notebook(
    path STRING,
    content STRING,
    language STRING DEFAULT 'PYTHON'
)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Create a notebook in the workspace with the given content.'
AS $$
    import json
    import base64
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.workspace import ImportFormat, Language
    try:
        w = WorkspaceClient()
        
        lang_map = {{
            "PYTHON": Language.PYTHON,
            "SQL": Language.SQL,
            "SCALA": Language.SCALA,
            "R": Language.R
        }}
        
        w.workspace.import_(
            path=path,
            content=base64.b64encode(content.encode()).decode(),
            format=ImportFormat.SOURCE,
            language=lang_map.get(language.upper(), Language.PYTHON),
            overwrite=True
        )
        
        return json.dumps({{"success": True, "path": path}})
    except Exception as e:
        return json.dumps({{"success": False, "error": str(e)}})
$$
""")
print("✓ create_notebook")

# COMMAND ----------

# Tool: List Workspace
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.list_workspace(path STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'List contents of a workspace folder.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        items = list(w.workspace.list(path))
        result = [{{
            "path": item.path,
            "type": str(item.object_type.value) if item.object_type else "UNKNOWN"
        }} for item in items[:50]]
        return json.dumps({{"path": path, "items": result}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ list_workspace")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Permissions

# COMMAND ----------

# Grant execute to all users
functions = [
    "execute_sql", "get_table_details", "list_warehouses",
    "create_dashboard", "list_dashboards",
    "create_job", "list_jobs", "run_job",
    "create_pipeline", "list_pipelines",
    "list_skills", "get_skill", "search_skills",
    "create_notebook", "list_workspace"
]

for func in functions:
    try:
        spark.sql(f"GRANT EXECUTE ON FUNCTION {CATALOG}.{SCHEMA}.{func} TO `account users`")
    except Exception as e:
        print(f"  Warning: Could not grant on {func}: {e}")

print("✓ Permissions granted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"""
{'='*60}
UC Tools Setup Complete!
{'='*60}

Tools created in: {CATALOG}.{SCHEMA}

SQL Tools:
  - execute_sql(query) - Run any SQL query
  - get_table_details(catalog, schema) - Explore tables
  - list_warehouses() - Find warehouse IDs

Dashboard Tools:
  - create_dashboard(name, path, json, warehouse_id) - Create AI/BI dashboards
  - list_dashboards() - List all dashboards

Job Tools:
  - create_job(name, notebook_path, cron, timezone) - Create jobs
  - list_jobs() - List all jobs
  - run_job(job_id) - Trigger a job

Pipeline Tools:
  - create_pipeline(name, catalog, schema, notebook) - Create DLT/SDP
  - list_pipelines() - List all pipelines

Skill Tools:
  - list_skills() - List available guidance
  - get_skill(name) - Get detailed documentation
  - search_skills(query) - Search for guidance

Workspace Tools:
  - create_notebook(path, content, language) - Create notebooks
  - list_workspace(path) - List folder contents

{'='*60}
Usage in Agent Mode:
  "Use execute_sql to show me sales data"
  "Create a dashboard for the trips table"
  "What skills are available for pipelines?"
{'='*60}
""")
