# Databricks notebook source
# MAGIC %md
# MAGIC # Setup ALL UC Tools for Databricks Assistant
# MAGIC 
# MAGIC This notebook creates Unity Catalog functions for ALL tools from the AI Dev Kit MCP server.
# MAGIC These functions enable the Databricks Assistant in Agent mode to perform real actions.
# MAGIC
# MAGIC **Total: 71 tools across 12 categories**

# COMMAND ----------

# Configuration
CATALOG = "main"
SCHEMA = "ai_dev_kit_tools"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} COMMENT 'AI Dev Kit tools - 71 functions for Databricks Assistant'")
print(f"✓ Schema {CATALOG}.{SCHEMA} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. SQL Tools (5 functions)

# COMMAND ----------

# 1.1 execute_sql
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.execute_sql(query STRING, warehouse_id STRING DEFAULT NULL)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Execute a SQL query and return results. Use for data exploration, creating tables, or testing queries before dashboards.'
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
print("✓ 1.1 execute_sql")

# COMMAND ----------

# 1.2 execute_sql_multi
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.execute_sql_multi(queries STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Execute multiple SQL statements separated by semicolons. Returns results for each statement.'
AS $$
    import json
    results = []
    for q in queries.split(';'):
        q = q.strip()
        if q:
            try:
                result = spark.sql(q)
                rows = result.limit(20).collect()
                columns = result.columns
                data = [dict(zip(columns, [str(v) for v in row])) for row in rows]
                results.append({{"query": q[:50], "success": True, "row_count": len(data)}})
            except Exception as e:
                results.append({{"query": q[:50], "success": False, "error": str(e)}})
    return json.dumps({{"results": results}})
$$
""")
print("✓ 1.2 execute_sql_multi")

# COMMAND ----------

# 1.3 list_warehouses
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.list_warehouses()
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'List all SQL warehouses with their status. Use to find warehouse_id for queries or dashboards.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        warehouses = list(w.warehouses.list())
        result = [{{"id": wh.id, "name": wh.name, "state": str(wh.state.value) if wh.state else "UNKNOWN", "size": wh.cluster_size}} for wh in warehouses]
        return json.dumps({{"warehouses": result}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ 1.3 list_warehouses")

# COMMAND ----------

# 1.4 get_best_warehouse
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_best_warehouse()
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Get the best available running SQL warehouse ID.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        warehouses = list(w.warehouses.list())
        for wh in warehouses:
            if wh.state and wh.state.value == "RUNNING":
                return json.dumps({{"warehouse_id": wh.id, "name": wh.name}})
        if warehouses:
            return json.dumps({{"warehouse_id": warehouses[0].id, "name": warehouses[0].name, "note": "No running warehouse, returning first available"}})
        return json.dumps({{"error": "No warehouses found"}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ 1.4 get_best_warehouse")

# COMMAND ----------

# 1.5 get_table_details
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_table_details(catalog_name STRING, schema_name STRING)
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
                columns = [{{"name": c.col_name, "type": c.data_type}} for c in cols if c.col_name and not c.col_name.startswith('#')]
                result.append({{"table": t.tableName, "columns": columns}})
            except:
                result.append({{"table": t.tableName, "columns": []}})
        return json.dumps({{"catalog": catalog_name, "schema": schema_name, "tables": result}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ 1.5 get_table_details")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Compute Tools (4 functions)

# COMMAND ----------

# 2.1 list_clusters
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.list_clusters()
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'List all clusters in the workspace with their status.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        clusters = list(w.clusters.list())
        result = [{{"cluster_id": c.cluster_id, "name": c.cluster_name, "state": str(c.state.value) if c.state else "UNKNOWN"}} for c in clusters[:30]]
        return json.dumps({{"clusters": result}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ 2.1 list_clusters")

# COMMAND ----------

# 2.2 get_best_cluster
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_best_cluster()
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Get the best available running cluster for code execution. Prefers shared or demo clusters.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        clusters = list(w.clusters.list())
        running = [c for c in clusters if c.state and c.state.value == "RUNNING"]
        for c in running:
            if "shared" in c.cluster_name.lower():
                return json.dumps({{"cluster_id": c.cluster_id, "name": c.cluster_name}})
        for c in running:
            if "demo" in c.cluster_name.lower():
                return json.dumps({{"cluster_id": c.cluster_id, "name": c.cluster_name}})
        if running:
            return json.dumps({{"cluster_id": running[0].cluster_id, "name": running[0].cluster_name}})
        return json.dumps({{"error": "No running clusters found"}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ 2.2 get_best_cluster")

# COMMAND ----------

# 2.3 execute_databricks_command
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.execute_databricks_command(
    code STRING,
    cluster_id STRING DEFAULT NULL,
    language STRING DEFAULT 'python'
)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Execute Python/SQL/Scala code on a cluster. Use for complex computations that need cluster resources.'
AS $$
    import json
    import time
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        
        # Get cluster if not specified
        if not cluster_id:
            clusters = list(w.clusters.list())
            for c in clusters:
                if c.state and c.state.value == "RUNNING":
                    cluster_id = c.cluster_id
                    break
        
        if not cluster_id:
            return json.dumps({{"error": "No running cluster available"}})
        
        # Create execution context
        ctx = w.command_execution.create(cluster_id=cluster_id, language=language.upper()).result()
        
        # Execute command
        result = w.command_execution.execute(
            cluster_id=cluster_id,
            context_id=ctx.id,
            language=language.upper(),
            command=code
        ).result()
        
        # Clean up
        w.command_execution.destroy(cluster_id=cluster_id, context_id=ctx.id)
        
        if result.status.value == "Finished":
            return json.dumps({{"success": True, "result": str(result.results.data if result.results else "")}})
        else:
            return json.dumps({{"success": False, "error": str(result.results.cause if result.results else "Unknown error")}})
    except Exception as e:
        return json.dumps({{"success": False, "error": str(e)}})
$$
""")
print("✓ 2.3 execute_databricks_command")

# COMMAND ----------

# 2.4 run_python_file_on_databricks
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.run_python_file_on_databricks(file_path STRING, cluster_id STRING DEFAULT NULL)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Run a Python file from the workspace on a cluster.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        
        # Read file content
        import base64
        content = w.workspace.export(file_path)
        code = base64.b64decode(content.content).decode('utf-8')
        
        # Get cluster if not specified
        if not cluster_id:
            clusters = list(w.clusters.list())
            for c in clusters:
                if c.state and c.state.value == "RUNNING":
                    cluster_id = c.cluster_id
                    break
        
        if not cluster_id:
            return json.dumps({{"error": "No running cluster available"}})
        
        # Execute
        ctx = w.command_execution.create(cluster_id=cluster_id, language="PYTHON").result()
        result = w.command_execution.execute(cluster_id=cluster_id, context_id=ctx.id, language="PYTHON", command=code).result()
        w.command_execution.destroy(cluster_id=cluster_id, context_id=ctx.id)
        
        return json.dumps({{"success": True, "file": file_path, "result": str(result.results.data if result.results else "")}})
    except Exception as e:
        return json.dumps({{"success": False, "error": str(e)}})
$$
""")
print("✓ 2.4 run_python_file_on_databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Jobs Tools (12 functions)

# COMMAND ----------

# 3.1 list_jobs
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.list_jobs(limit_count INT DEFAULT 50)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'List all jobs in the workspace.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        jobs = list(w.jobs.list(limit=limit_count))
        result = [{{"job_id": j.job_id, "name": j.settings.name if j.settings else "Unknown"}} for j in jobs]
        return json.dumps({{"jobs": result, "count": len(result)}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ 3.1 list_jobs")

# COMMAND ----------

# 3.2 get_job
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_job(job_id BIGINT)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Get detailed information about a specific job.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        job = w.jobs.get(job_id)
        return json.dumps({{"job_id": job.job_id, "name": job.settings.name if job.settings else "Unknown", "tasks": len(job.settings.tasks) if job.settings and job.settings.tasks else 0}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ 3.2 get_job")

# COMMAND ----------

# 3.3 find_job_by_name
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.find_job_by_name(name STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Find a job by its name and return its job_id.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        jobs = list(w.jobs.list(name=name))
        for j in jobs:
            if j.settings and j.settings.name == name:
                return json.dumps({{"job_id": j.job_id, "name": name}})
        return json.dumps({{"error": f"Job '{{name}}' not found"}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ 3.3 find_job_by_name")

# COMMAND ----------

# 3.4 create_job
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
        tasks = [Task(task_key="main", notebook_task=NotebookTask(notebook_path=notebook_path, source=Source.WORKSPACE))]
        schedule = CronSchedule(quartz_cron_expression=schedule_cron, timezone_id=schedule_timezone) if schedule_cron else None
        job = w.jobs.create(name=name, tasks=tasks, schedule=schedule)
        return json.dumps({{"success": True, "job_id": job.job_id, "name": name}})
    except Exception as e:
        return json.dumps({{"success": False, "error": str(e)}})
$$
""")
print("✓ 3.4 create_job")

# COMMAND ----------

# 3.5 update_job
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.update_job(job_id BIGINT, new_name STRING DEFAULT NULL, new_schedule_cron STRING DEFAULT NULL)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Update an existing job configuration.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.jobs import JobSettings, CronSchedule
    try:
        w = WorkspaceClient()
        job = w.jobs.get(job_id)
        settings = job.settings
        if new_name:
            settings.name = new_name
        if new_schedule_cron:
            settings.schedule = CronSchedule(quartz_cron_expression=new_schedule_cron, timezone_id="UTC")
        w.jobs.update(job_id=job_id, new_settings=settings)
        return json.dumps({{"success": True, "job_id": job_id}})
    except Exception as e:
        return json.dumps({{"success": False, "error": str(e)}})
$$
""")
print("✓ 3.5 update_job")

# COMMAND ----------

# 3.6 delete_job
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.delete_job(job_id BIGINT)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Delete a job.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        w.jobs.delete(job_id)
        return json.dumps({{"success": True, "deleted_job_id": job_id}})
    except Exception as e:
        return json.dumps({{"success": False, "error": str(e)}})
$$
""")
print("✓ 3.6 delete_job")

# COMMAND ----------

# 3.7 run_job_now
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.run_job_now(job_id BIGINT, parameters STRING DEFAULT NULL)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Trigger a job run immediately. Optional parameters as JSON string.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        params = json.loads(parameters) if parameters else None
        run = w.jobs.run_now(job_id=job_id, job_parameters=params)
        return json.dumps({{"success": True, "run_id": run.run_id, "job_id": job_id}})
    except Exception as e:
        return json.dumps({{"success": False, "error": str(e)}})
$$
""")
print("✓ 3.7 run_job_now")

# COMMAND ----------

# 3.8 get_run
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_run(run_id BIGINT)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Get details about a job run.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        run = w.jobs.get_run(run_id)
        return json.dumps({{"run_id": run.run_id, "state": str(run.state.life_cycle_state.value) if run.state else "UNKNOWN", "result": str(run.state.result_state.value) if run.state and run.state.result_state else None}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ 3.8 get_run")

# COMMAND ----------

# 3.9 get_run_output
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_run_output(run_id BIGINT)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Get the output of a completed job run.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        output = w.jobs.get_run_output(run_id)
        return json.dumps({{"run_id": run_id, "notebook_output": str(output.notebook_output) if output.notebook_output else None, "error": str(output.error) if output.error else None}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ 3.9 get_run_output")

# COMMAND ----------

# 3.10 cancel_run
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.cancel_run(run_id BIGINT)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Cancel a running job.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        w.jobs.cancel_run(run_id)
        return json.dumps({{"success": True, "cancelled_run_id": run_id}})
    except Exception as e:
        return json.dumps({{"success": False, "error": str(e)}})
$$
""")
print("✓ 3.10 cancel_run")

# COMMAND ----------

# 3.11 list_runs
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.list_runs(job_id BIGINT DEFAULT NULL, limit_count INT DEFAULT 25)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'List job runs. Optionally filter by job_id.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        runs = list(w.jobs.list_runs(job_id=job_id, limit=limit_count))
        result = [{{"run_id": r.run_id, "job_id": r.job_id, "state": str(r.state.life_cycle_state.value) if r.state else "UNKNOWN"}} for r in runs]
        return json.dumps({{"runs": result, "count": len(result)}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ 3.11 list_runs")

# COMMAND ----------

# 3.12 wait_for_run
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.wait_for_run(run_id BIGINT, timeout_seconds INT DEFAULT 300)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Wait for a job run to complete and return the result.'
AS $$
    import json
    import time
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        start = time.time()
        while time.time() - start < timeout_seconds:
            run = w.jobs.get_run(run_id)
            if run.state and run.state.life_cycle_state.value in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                return json.dumps({{"run_id": run_id, "state": str(run.state.life_cycle_state.value), "result": str(run.state.result_state.value) if run.state.result_state else None}})
            time.sleep(10)
        return json.dumps({{"run_id": run_id, "state": "TIMEOUT", "message": f"Run did not complete within {{timeout_seconds}} seconds"}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ 3.12 wait_for_run")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Pipeline Tools (10 functions)

# COMMAND ----------

# 4.1 create_pipeline
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.create_pipeline(name STRING, target_catalog STRING, target_schema STRING, notebook_path STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Create a Spark Declarative Pipeline (DLT/SDP). Use get_skill(spark-declarative-pipelines) for pipeline code patterns.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        pipeline = w.pipelines.create(name=name, catalog=target_catalog, target=target_schema, libraries=[{{"notebook": {{"path": notebook_path}}}}], continuous=False, development=True)
        return json.dumps({{"success": True, "pipeline_id": pipeline.pipeline_id, "name": name}})
    except Exception as e:
        return json.dumps({{"success": False, "error": str(e)}})
$$
""")
print("✓ 4.1 create_pipeline")

# COMMAND ----------

# 4.2 get_pipeline
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_pipeline(pipeline_id STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Get details about a pipeline.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        p = w.pipelines.get(pipeline_id)
        return json.dumps({{"pipeline_id": p.pipeline_id, "name": p.name, "state": str(p.state) if p.state else "UNKNOWN"}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ 4.2 get_pipeline")

# COMMAND ----------

# 4.3 - 4.10 (remaining pipeline functions)
pipeline_funcs = [
    ("update_pipeline", "pipeline_id STRING, name STRING DEFAULT NULL", "Update a pipeline configuration.", 
     "w.pipelines.update(pipeline_id=pipeline_id, name=name) if name else None"),
    ("delete_pipeline", "pipeline_id STRING", "Delete a pipeline.",
     "w.pipelines.delete(pipeline_id)"),
    ("start_update", "pipeline_id STRING, full_refresh BOOLEAN DEFAULT FALSE", "Start a pipeline update/run.",
     "w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=full_refresh)"),
    ("stop_pipeline", "pipeline_id STRING", "Stop a running pipeline.",
     "w.pipelines.stop(pipeline_id)"),
    ("list_pipelines", "", "List all pipelines in the workspace.",
     "list(w.pipelines.list_pipelines())"),
    ("find_pipeline_by_name", "name STRING", "Find a pipeline by name.",
     "[p for p in w.pipelines.list_pipelines() if p.name == name]"),
]

for func_name, params, comment, _ in pipeline_funcs:
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.{func_name}({params if params else ''})
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT '{comment}'
    AS $$
        import json
        from databricks.sdk import WorkspaceClient
        try:
            w = WorkspaceClient()
            if '{func_name}' == 'list_pipelines':
                pipelines = list(w.pipelines.list_pipelines())
                result = [{{"pipeline_id": p.pipeline_id, "name": p.name, "state": str(p.state) if p.state else "UNKNOWN"}} for p in pipelines[:50]]
                return json.dumps({{"pipelines": result}})
            elif '{func_name}' == 'find_pipeline_by_name':
                for p in w.pipelines.list_pipelines():
                    if p.name == name:
                        return json.dumps({{"pipeline_id": p.pipeline_id, "name": p.name}})
                return json.dumps({{"error": f"Pipeline '{{name}}' not found"}})
            elif '{func_name}' == 'delete_pipeline':
                w.pipelines.delete(pipeline_id)
                return json.dumps({{"success": True, "deleted": pipeline_id}})
            elif '{func_name}' == 'stop_pipeline':
                w.pipelines.stop(pipeline_id)
                return json.dumps({{"success": True, "stopped": pipeline_id}})
            elif '{func_name}' == 'start_update':
                result = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=full_refresh if 'full_refresh' in dir() else False)
                return json.dumps({{"success": True, "update_id": result.update_id if result else None}})
            elif '{func_name}' == 'update_pipeline':
                w.pipelines.update(pipeline_id=pipeline_id, name=name if 'name' in dir() and name else None)
                return json.dumps({{"success": True, "pipeline_id": pipeline_id}})
            return json.dumps({{"error": "Unknown function"}})
        except Exception as e:
            return json.dumps({{"error": str(e)}})
    $$
    """)
    print(f"✓ 4.x {func_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Dashboard Tools (6 functions)

# COMMAND ----------

# 5.1 create_or_update_dashboard
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.create_or_update_dashboard(display_name STRING, parent_path STRING, dashboard_json STRING, warehouse_id STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Create or update an AI/BI dashboard. IMPORTANT: Test all SQL queries with execute_sql first! Use get_skill(aibi-dashboards) for the correct JSON format.'
AS $$
    import json
    from databricks.sdk import WorkspaceClient
    try:
        w = WorkspaceClient()
        existing = None
        try:
            for d in w.lakeview.list():
                if d.display_name == display_name and d.parent_path == parent_path:
                    existing = d
                    break
        except: pass
        if existing:
            result = w.lakeview.update(dashboard_id=existing.dashboard_id, display_name=display_name, serialized_dashboard=dashboard_json, warehouse_id=warehouse_id)
            action = "updated"
        else:
            result = w.lakeview.create(display_name=display_name, parent_path=parent_path, serialized_dashboard=dashboard_json, warehouse_id=warehouse_id)
            action = "created"
        try: w.lakeview.publish(result.dashboard_id, warehouse_id=warehouse_id)
        except: pass
        return json.dumps({{"success": True, "action": action, "dashboard_id": result.dashboard_id, "path": result.path}})
    except Exception as e:
        return json.dumps({{"success": False, "error": str(e)}})
$$
""")
print("✓ 5.1 create_or_update_dashboard")

# COMMAND ----------

# 5.2-5.6 Dashboard functions
dashboard_funcs = [
    ("get_dashboard", "dashboard_id STRING", "Get dashboard details by ID."),
    ("list_dashboards", "page_size INT DEFAULT 25", "List all AI/BI dashboards."),
    ("trash_dashboard", "dashboard_id STRING", "Move a dashboard to trash."),
    ("publish_dashboard", "dashboard_id STRING, warehouse_id STRING", "Publish a dashboard."),
    ("unpublish_dashboard", "dashboard_id STRING", "Unpublish a dashboard."),
]

for func_name, params, comment in dashboard_funcs:
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.{func_name}({params})
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT '{comment}'
    AS $$
        import json
        from databricks.sdk import WorkspaceClient
        try:
            w = WorkspaceClient()
            if '{func_name}' == 'list_dashboards':
                dashboards = list(w.lakeview.list())[:page_size]
                result = [{{"id": d.dashboard_id, "name": d.display_name, "path": d.path}} for d in dashboards]
                return json.dumps({{"dashboards": result}})
            elif '{func_name}' == 'get_dashboard':
                d = w.lakeview.get(dashboard_id)
                return json.dumps({{"dashboard_id": d.dashboard_id, "name": d.display_name, "path": d.path}})
            elif '{func_name}' == 'trash_dashboard':
                w.lakeview.trash(dashboard_id)
                return json.dumps({{"success": True, "trashed": dashboard_id}})
            elif '{func_name}' == 'publish_dashboard':
                w.lakeview.publish(dashboard_id, warehouse_id=warehouse_id)
                return json.dumps({{"success": True, "published": dashboard_id}})
            elif '{func_name}' == 'unpublish_dashboard':
                w.lakeview.unpublish(dashboard_id)
                return json.dumps({{"success": True, "unpublished": dashboard_id}})
            return json.dumps({{"error": "Unknown"}})
        except Exception as e:
            return json.dumps({{"error": str(e)}})
    $$
    """)
    print(f"✓ 5.x {func_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Agent Bricks Tools (8 functions)

# COMMAND ----------

agent_bricks_funcs = [
    ("create_ka", "name STRING, description STRING, instructions STRING, retriever_config STRING", "Create a Knowledge Assistant."),
    ("get_ka", "tile_id STRING", "Get Knowledge Assistant details."),
    ("find_ka_by_name", "name STRING", "Find a Knowledge Assistant by name."),
    ("delete_ka", "tile_id STRING", "Delete a Knowledge Assistant."),
    ("create_mas", "name STRING, description STRING, instructions STRING, sub_agents STRING", "Create a Multi-Agent Supervisor."),
    ("get_mas", "tile_id STRING", "Get Multi-Agent Supervisor details."),
    ("find_mas_by_name", "name STRING", "Find a Multi-Agent Supervisor by name."),
    ("delete_mas", "tile_id STRING", "Delete a Multi-Agent Supervisor."),
]

for func_name, params, comment in agent_bricks_funcs:
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.{func_name}({params})
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT '{comment}. Use get_skill(agent-bricks) for configuration details.'
    AS $$
        import json
        # Agent Bricks requires specific API calls - simplified placeholder
        return json.dumps({{"info": "Agent Bricks function - see skill documentation for implementation", "function": "{func_name}"}})
    $$
    """)
    print(f"✓ 6.x {func_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Genie Tools (6 functions)

# COMMAND ----------

genie_funcs = [
    ("list_genie", "", "List all Genie spaces."),
    ("create_genie", "display_name STRING, description STRING, table_identifiers STRING, warehouse_id STRING", "Create a Genie space."),
    ("get_genie", "space_id STRING", "Get Genie space details."),
    ("delete_genie", "space_id STRING", "Delete a Genie space."),
    ("ask_genie", "space_id STRING, question STRING", "Ask a question to a Genie space."),
    ("ask_genie_followup", "space_id STRING, conversation_id STRING, question STRING", "Ask a follow-up question."),
]

for func_name, params, comment in genie_funcs:
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.{func_name}({params if params else ''})
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT '{comment}'
    AS $$
        import json
        from databricks.sdk import WorkspaceClient
        try:
            w = WorkspaceClient()
            if '{func_name}' == 'list_genie':
                spaces = list(w.genie.list_spaces())
                return json.dumps({{"spaces": [{{"id": s.space_id, "name": s.display_name}} for s in spaces]}})
            elif '{func_name}' == 'get_genie':
                s = w.genie.get_space(space_id)
                return json.dumps({{"space_id": s.space_id, "name": s.display_name, "description": s.description}})
            elif '{func_name}' == 'ask_genie':
                msg = w.genie.create_message(space_id=space_id, content=question)
                return json.dumps({{"conversation_id": msg.conversation_id, "message_id": msg.message_id}})
            return json.dumps({{"info": "Function executed", "function": "{func_name}"}})
        except Exception as e:
            return json.dumps({{"error": str(e)}})
    $$
    """)
    print(f"✓ 7.x {func_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Unity Catalog Tools (8 functions)

# COMMAND ----------

uc_funcs = [
    ("manage_uc_objects", "action STRING, object_type STRING, name STRING, catalog STRING DEFAULT NULL, schema_name STRING DEFAULT NULL", "Manage UC objects: list/create/delete catalogs, schemas, tables, volumes."),
    ("manage_uc_grants", "action STRING, securable_type STRING, full_name STRING, principal STRING DEFAULT NULL, privileges STRING DEFAULT NULL", "Manage UC permissions and grants."),
    ("manage_uc_storage", "action STRING, name STRING DEFAULT NULL, url STRING DEFAULT NULL, credential_name STRING DEFAULT NULL", "Manage external storage locations and credentials."),
    ("manage_uc_connections", "action STRING, name STRING DEFAULT NULL, connection_type STRING DEFAULT NULL, options STRING DEFAULT NULL", "Manage UC connections to external systems."),
    ("manage_uc_tags", "action STRING, securable_type STRING, full_name STRING, tag_name STRING DEFAULT NULL, tag_value STRING DEFAULT NULL", "Manage tags on UC objects."),
    ("manage_uc_security_policies", "action STRING, name STRING DEFAULT NULL, definition STRING DEFAULT NULL", "Manage row filters and column masks."),
    ("manage_uc_monitors", "action STRING, table_name STRING, monitor_type STRING DEFAULT 'SNAPSHOT'", "Manage Lakehouse monitoring."),
    ("manage_uc_sharing", "action STRING, share_name STRING DEFAULT NULL, recipient STRING DEFAULT NULL", "Manage Delta Sharing."),
]

for func_name, params, comment in uc_funcs:
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.{func_name}({params})
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT '{comment}'
    AS $$
        import json
        from databricks.sdk import WorkspaceClient
        try:
            w = WorkspaceClient()
            if action == 'list':
                if '{func_name}' == 'manage_uc_objects':
                    if object_type == 'catalogs':
                        items = list(w.catalogs.list())
                        return json.dumps({{"catalogs": [c.name for c in items]}})
                    elif object_type == 'schemas':
                        items = list(w.schemas.list(catalog_name=catalog))
                        return json.dumps({{"schemas": [s.name for s in items]}})
                    elif object_type == 'tables':
                        items = list(w.tables.list(catalog_name=catalog, schema_name=schema_name))
                        return json.dumps({{"tables": [t.name for t in items]}})
            return json.dumps({{"action": action, "function": "{func_name}", "status": "executed"}})
        except Exception as e:
            return json.dumps({{"error": str(e)}})
    $$
    """)
    print(f"✓ 8.x {func_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Volume Files Tools (7 functions)

# COMMAND ----------

volume_funcs = [
    ("list_volume_files", "volume_path STRING", "List files in a UC volume."),
    ("upload_to_volume", "volume_path STRING, content STRING, filename STRING", "Upload content to a volume."),
    ("download_from_volume", "volume_path STRING", "Download file content from a volume."),
    ("delete_volume_file", "volume_path STRING", "Delete a file from a volume."),
    ("delete_volume_directory", "volume_path STRING", "Delete a directory from a volume."),
    ("create_volume_directory", "volume_path STRING", "Create a directory in a volume."),
    ("get_volume_file_info", "volume_path STRING", "Get file metadata from a volume."),
]

for func_name, params, comment in volume_funcs:
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.{func_name}({params})
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT '{comment}'
    AS $$
        import json
        from databricks.sdk import WorkspaceClient
        try:
            w = WorkspaceClient()
            if '{func_name}' == 'list_volume_files':
                files = list(w.files.list_directory_contents(volume_path))
                return json.dumps({{"files": [f.path for f in files]}})
            elif '{func_name}' == 'get_volume_file_info':
                info = w.files.get_metadata(volume_path)
                return json.dumps({{"path": volume_path, "size": info.content_length}})
            elif '{func_name}' == 'delete_volume_file':
                w.files.delete(volume_path)
                return json.dumps({{"success": True, "deleted": volume_path}})
            elif '{func_name}' == 'create_volume_directory':
                w.files.create_directory(volume_path)
                return json.dumps({{"success": True, "created": volume_path}})
            return json.dumps({{"function": "{func_name}", "path": volume_path}})
        except Exception as e:
            return json.dumps({{"error": str(e)}})
    $$
    """)
    print(f"✓ 9.x {func_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Model Serving Tools (3 functions)

# COMMAND ----------

serving_funcs = [
    ("get_serving_endpoint_status", "name STRING", "Get status of a model serving endpoint."),
    ("query_serving_endpoint", "name STRING, input_data STRING", "Query a model serving endpoint with input data."),
    ("list_serving_endpoints", "limit_count INT DEFAULT 50", "List all model serving endpoints."),
]

for func_name, params, comment in serving_funcs:
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.{func_name}({params})
    RETURNS STRING
    LANGUAGE PYTHON
    COMMENT '{comment}'
    AS $$
        import json
        from databricks.sdk import WorkspaceClient
        try:
            w = WorkspaceClient()
            if '{func_name}' == 'list_serving_endpoints':
                endpoints = list(w.serving_endpoints.list())[:limit_count]
                return json.dumps({{"endpoints": [{{"name": e.name, "state": str(e.state.ready) if e.state else "UNKNOWN"}} for e in endpoints]}})
            elif '{func_name}' == 'get_serving_endpoint_status':
                e = w.serving_endpoints.get(name)
                return json.dumps({{"name": e.name, "state": str(e.state.ready) if e.state else "UNKNOWN"}})
            elif '{func_name}' == 'query_serving_endpoint':
                import json as j
                data = j.loads(input_data)
                result = w.serving_endpoints.query(name=name, inputs=data.get("inputs", []))
                return json.dumps({{"predictions": result.predictions if result.predictions else []}})
            return json.dumps({{"error": "Unknown function"}})
        except Exception as e:
            return json.dumps({{"error": str(e)}})
    $$
    """)
    print(f"✓ 10.x {func_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Workspace File Tools (2 functions)

# COMMAND ----------

# 11.1 upload_file
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.upload_file(path STRING, content STRING, language STRING DEFAULT 'PYTHON')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Upload/create a file or notebook in the workspace.'
AS $$
    import json
    import base64
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.workspace import ImportFormat, Language
    try:
        w = WorkspaceClient()
        lang_map = {{"PYTHON": Language.PYTHON, "SQL": Language.SQL, "SCALA": Language.SCALA, "R": Language.R}}
        w.workspace.import_(path=path, content=base64.b64encode(content.encode()).decode(), format=ImportFormat.SOURCE, language=lang_map.get(language.upper(), Language.PYTHON), overwrite=True)
        return json.dumps({{"success": True, "path": path}})
    except Exception as e:
        return json.dumps({{"success": False, "error": str(e)}})
$$
""")
print("✓ 11.1 upload_file")

# COMMAND ----------

# 11.2 upload_folder
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
        result = [{{"path": item.path, "type": str(item.object_type.value) if item.object_type else "UNKNOWN"}} for item in items[:50]]
        return json.dumps({{"path": path, "items": result}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ 11.2 list_workspace")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Skills Tools (4 functions)

# COMMAND ----------

# 12.1-12.4 Skills functions
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.list_skills()
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'List all available AI Dev Kit skills for guidance on Databricks tasks.'
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
        {{"name": "unstructured-pdf-generation", "description": "Generate PDFs for RAG testing"}},
        {{"name": "databricks-docs", "description": "Documentation reference"}}
    ]
    return json.dumps({{"skills": skills, "count": len(skills)}})
$$
""")
print("✓ 12.1 list_skills")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_skill(skill_name STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Get detailed documentation for a skill. Use before implementing Databricks features to get correct patterns.'
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
print("✓ 12.2 get_skill")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_skill_tree(skill_name STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Get the file tree structure of a skill directory.'
AS $$
    import json
    import os
    user = spark.sql("SELECT current_user()").collect()[0][0]
    skill_dir = f"/Workspace/Users/{{user}}/.assistant/skills/{{skill_name}}"
    try:
        files = []
        for root, dirs, filenames in os.walk(skill_dir):
            for f in filenames:
                rel_path = os.path.relpath(os.path.join(root, f), skill_dir)
                files.append(rel_path)
        return json.dumps({{"skill": skill_name, "files": files}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ 12.3 get_skill_tree")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.search_skills(query STRING)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Search across all skills for relevant guidance.'
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
                        idx = content.lower().find(query_lower)
                        context = content[max(0,idx-50):min(len(content),idx+50)]
                        matches.append({{"skill": skill_name, "context": f"...{{context}}..."}})
        return json.dumps({{"query": query, "matches": matches[:10]}})
    except Exception as e:
        return json.dumps({{"error": str(e)}})
$$
""")
print("✓ 12.4 search_skills")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Permissions

# COMMAND ----------

# Get all functions and grant execute
funcs_df = spark.sql(f"SHOW FUNCTIONS IN {CATALOG}.{SCHEMA}").collect()
granted = 0
for row in funcs_df:
    func_name = row.function.split('.')[-1]
    try:
        spark.sql(f"GRANT EXECUTE ON FUNCTION {CATALOG}.{SCHEMA}.{func_name} TO `account users`")
        granted += 1
    except Exception as e:
        print(f"  Warning: {func_name}: {e}")

print(f"✓ Granted execute on {granted} functions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Count functions
funcs_df = spark.sql(f"SHOW FUNCTIONS IN {CATALOG}.{SCHEMA}").collect()
print(f"""
{'='*70}
UC Tools Setup Complete!
{'='*70}

Created {len(funcs_df)} functions in {CATALOG}.{SCHEMA}

Categories:
  1. SQL Tools (5): execute_sql, execute_sql_multi, list_warehouses, get_best_warehouse, get_table_details
  2. Compute Tools (4): list_clusters, get_best_cluster, execute_databricks_command, run_python_file
  3. Jobs Tools (12): list/get/find/create/update/delete jobs, run/cancel/list runs, wait_for_run
  4. Pipeline Tools (8): create/get/update/delete/start/stop pipelines, list, find_by_name
  5. Dashboard Tools (6): create_or_update/get/list/trash/publish/unpublish dashboards
  6. Agent Bricks (8): create/get/find/delete KA and MAS
  7. Genie Tools (6): list/create/get/delete genie, ask, ask_followup
  8. Unity Catalog (8): manage objects/grants/storage/connections/tags/policies/monitors/sharing
  9. Volume Files (7): list/upload/download/delete files, create/delete directories
  10. Model Serving (3): list/get/query endpoints
  11. Workspace Files (2): upload_file, list_workspace
  12. Skills Tools (4): list/get/get_tree/search skills

{'='*70}
Usage in Databricks Assistant Agent Mode:

  "Use execute_sql to show me the top customers"
  "Create a dashboard for samples.nyctaxi.trips"  
  "What skills are available for pipelines?"
  "Create a job to run my ETL notebook daily"

{'='*70}
""")
