/* ============================================================
   SECTION 1: Define time window (last 24 hours)
   ============================================================ */
DECLARE @StartTime DATETIME2 = DATEADD(HOUR, -24, SYSUTCDATETIME());

/* ============================================================
   SECTION 2: System-level errors from sys.event_log
   Captures:
     - Engine errors
     - Deadlocks
     - Throttling
     - Failovers
     - Connection issues
   ============================================================ */
SELECT 
    'SYSTEM_ERROR' AS Source,
    event_time,
    database_name,
    error_number,
    severity,
    event_type,
    message,
    additional_data
FROM sys.event_log
WHERE event_time >= @StartTime
  AND (error_number IS NOT NULL OR severity >= 11);


/* ============================================================
   SECTION 3: Deadlocks (explicit filter)
   ============================================================ */
SELECT
    'DEADLOCK' AS Source,
    event_time,
    database_name,
    event_type,
    message,
    additional_data
FROM sys.event_log
WHERE event_time >= @StartTime
  AND event_type = 'deadlock';


/* ============================================================
   SECTION 4: Throttling events (DTU/CPU/IO pressure)
   ============================================================ */
SELECT
    'THROTTLING' AS Source,
    event_time,
    database_name,
    event_type,
    message,
    additional_data
FROM sys.event_log
WHERE event_time >= @StartTime
  AND event_type LIKE '%throttle%';


/* ============================================================
   SECTION 5: Connection failures
   ============================================================ */
SELECT
    'CONNECTION_FAILURE' AS Source,
    event_time,
    database_name,
    event_type,
    message,
    additional_data
FROM sys.event_log
WHERE event_time >= @StartTime
  AND event_type LIKE '%connection%';


/* ============================================================
   SECTION 6: Query-level errors from sys.dm_exec_query_stats
   Captures:
     - Query text
     - Last error number
     - Last error message
     - Execution count
     - Last execution time
   ============================================================ */
SELECT
    'QUERY_ERROR' AS Source,
    qs.last_execution_time AS event_time,
    DB_NAME() AS database_name,
    qs.last_error_number AS error_number,
    qs.last_error_severity AS severity,
    qs.last_error_message AS message,
    qs.execution_count,
    SUBSTRING(
        qt.text,
        qs.statement_start_offset/2,
        (CASE WHEN qs.statement_end_offset = -1 
              THEN LEN(qt.text) * 2 
              ELSE qs.statement_end_offset END - qs.statement_start_offset)/2
    ) AS query_text
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
WHERE qs.last_execution_time >= @StartTime
  AND qs.last_error_number IS NOT NULL
ORDER BY qs.last_execution_time DESC;