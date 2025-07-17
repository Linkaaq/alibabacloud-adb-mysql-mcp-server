import asyncio
import os

import pymysql
from mcp.server import Server
from mcp.types import Resource, ResourceTemplate, Tool, TextContent
from pydantic import AnyUrl

import aiohttp
from aiohttp import web


def get_db_config():
    config = {
        "host": os.getenv("ADB_MYSQL_HOST", "localhost"),
        "port": int(os.getenv("ADB_MYSQL_PORT", 3306)),
        "user": os.getenv("ADB_MYSQL_USER"),
        "password": os.getenv("ADB_MYSQL_PASSWORD"),
        "database": os.getenv("ADB_MYSQL_DATABASE"),
    }

    if not all([config["user"], config["password"], config["database"]]):
        raise ValueError("Missing required database configuration")

    return config


mcp_app = Server(
    name="adb-mysql-mcp-server",
    version="1.0.0"
)


@mcp_app.list_resources()
async def list_resources() -> list[Resource]:
    return [
        Resource(
            uri="adbmysql:///databases",
            name="All of the databases",
            description="Display all of the databases in Adb MySQL",
            mimeType="text/plain"
        )
    ]


@mcp_app.list_resource_templates()
async def list_resource_templates() -> list[ResourceTemplate]:
    return [
        ResourceTemplate(
            uriTemplate="adbmysql:///{database}/tables",
            name="Database Tables",
            description="Get all the tables in a specific database",
            mimeType="text/plain"
        ),
        ResourceTemplate(
            uriTemplate="adbmysql:///{database}/{table}/ddl",
            name="Table DDL",
            description="Get the DDL script of a table in a specific database",
            mimeType="text/plain"
        ),
        ResourceTemplate(
            uriTemplate="adbmysql:///config/{key}/value",
            name="Database Config",
            description="Get the value for a config key in the cluster",
            mimeType="text/plain"
        ),
    ]


@mcp_app.read_resource()
async def read_resource(uri: AnyUrl) -> str:
    config = get_db_config()
    uri_str = str(uri)

    if not uri_str.startswith("adbmysql:///"):
        raise ValueError(f"Invalid URI: {uri_str}")

    conn = pymysql.connect(**config)
    conn.autocommit(True)
    cursor = conn.cursor()

    try:
        if uri_str.startswith("adbmysql:///"):
            paths = uri_str[12:].split("/")
            if paths[0] == "databases":
                query = "show databases;"
                cursor.execute(query)
                databases = cursor.fetchall()
                return "\n".join([database[0] for database in databases])
            elif len(paths) == 2 and paths[1] == "tables":
                database = paths[0]
                query = f"show tables from {database};"
                cursor.execute(query)
                tables = cursor.fetchall()
                return "\n".join([table[0] for table in tables])
            elif len(paths) == 3 and paths[2] == "ddl":
                database = paths[0]
                table = paths[1]
                query = f"show create table {database}.{table};"
                cursor.execute(query)
                ddl = cursor.fetchone()
                return ddl[1] if ddl and ddl[1] else f"No DDL Found for {database}.{table}"
            elif len(paths) == 3 and paths[0] == "config" and paths[2] == "value":
                key = paths[1]
                query = f"""show adb_config key={key}"""
                cursor.execute(query)
                value = cursor.fetchone()
                return value[1] if value and value[1] else f"No Config Value Found for {key}"
            else:
                raise ValueError(f"Invalid mcp resource URI format:  {uri_str}")
        else:
            raise ValueError(f"Invalid resource URI format:  {uri_str}")

    except pymysql.Error as e:
        raise RuntimeError(f"Database error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn.open:
            conn.close()


@mcp_app.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="execute_sql",
            description="Execute a SQL query in the Adb MySQL Cluster",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SQL query to execute"
                    }
                },
                "required": ["query"]
            },
        ),
        Tool(
            name="get_query_plan",
            description="Get the query plan for a SQL query",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SQL query to analyze"
                    }
                },
                "required": ["query"]
            },
        ),
        Tool(
            name="get_execution_plan",
            description="Get the actual execution plan with runtime statistics for a SQL query",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SQL query to analyze"
                    }
                },
                "required": ["query"]
            }
        )
    ]


@mcp_app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Execute SQL commands."""
    config = get_db_config()

    if name == "execute_sql":
        query = arguments.get("query")
        if not query:
            raise ValueError("Query is required")
    elif name == "get_query_plan":
        query = arguments.get("query")
        if not query:
            raise ValueError("Query is required")
        query = f"EXPLAIN {query}"
    elif name == "get_execution_plan":
        query = arguments.get("query")
        if not query:
            raise ValueError("Query is required")
        query = f"EXPLAIN ANALYZE {query}"
    else:
        raise ValueError(f"Unknown tool: {name}")

    conn = pymysql.connect(**config)
    conn.autocommit(True)
    cursor = conn.cursor()

    try:
        # Execute the query
        cursor.execute(query)

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        result = [",".join(map(str, row)) for row in rows]
        return [TextContent(type="text", text="\n".join([",".join(columns)] + result))]
    except Exception as e:
        return [TextContent(type="text", text=f"Error executing query: {str(e)}")]
    finally:
        if cursor:
            cursor.close()
        if conn.open:
            conn.close()


async def sse_handler(request):
    # 设置SSE响应头
    resp = web.StreamResponse(
        status=200,
        reason='OK',
        headers={
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
        },
    )
    await resp.prepare(request)
    
    # 创建输入输出流适配器
    class SSEWriter:
        def __init__(self, resp):
            self.resp = resp
        
        async def write(self, data):
            # 将MCP输出包装为SSE事件格式
            sse_data = f"data: {data}\n\n"
            await self.resp.write(sse_data.encode('utf-8'))
    
    writer = SSEWriter(resp)
    
    try:
        await mcp_app.run(
            request.content,  # read_stream
            writer,           # write_stream
            mcp_app.create_initialization_options()
        )
    except Exception as e:
        await writer.write(f"Error: {str(e)}")
    finally:
        await resp.write_eof()
    
    return resp

async def main():
    # 创建aiohttp应用
    web_app = web.Application()
    web_app.add_routes([web.get('/events', sse_handler)])
    
    # 启动服务器
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', 3000)
    await site.start()
    
    # 保持服务器运行
    print("SSE server running on http://localhost:3000/events")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
