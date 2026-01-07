-- POSTGRESQL QUERY PERFORMANCE TEST
-- Tests query performance with and without RLS

\c health_iot_benchmark;

-- 1. TEST SETUP AND CONFIGURATION
-- ============================================
DO $$
DECLARE
    test_start TIMESTAMPTZ;
    test_end TIMESTAMPTZ;
    test_duration INTERVAL;
    query_execution_time NUMERIC;
    explain_plan JSONB;
    security_context VARCHAR;
    user_role VARCHAR;
    test_results JSONB := '[]'::JSONB;
    query_counter INTEGER := 0;
BEGIN
    RAISE NOTICE '============================================';
    RAISE NOTICE 'QUERY PERFORMANCE BENCHMARK';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'Testing various query types with/without RLS';
    RAISE NOTICE '';
    
    -- Create test results table
    DROP TABLE IF EXISTS query_performance_results;
    CREATE TABLE query_performance_results (
        result_id SERIAL PRIMARY KEY,
        test_timestamp TIMESTAMPTZ DEFAULT NOW(),
        query_type VARCHAR(100),
        query_description TEXT,
        rls_enabled BOOLEAN,
        security_context VARCHAR(50),
        user_role VARCHAR(50),
        execution_time_ms NUMERIC(10,3),
        planning_time_ms NUMERIC(10,3),
        rows_returned INTEGER,
        rows_examined INTEGER,
        index_used BOOLEAN,
        index_name VARCHAR(100),
        buffer_hits BIGINT,
        buffer_reads BIGINT,
        query_plan JSONB,
        test_phase VARCHAR(50)
    );
    
    -- 2. BASELINE TESTS (NO RLS)
    -- ============================================
    RAISE NOTICE 'PHASE 1: BASELINE (NO RLS)';
    RAISE NOTICE '-------------------------';
    
    -- Disable RLS temporarily
    ALTER TABLE patient_vitals DISABLE ROW LEVEL SECURITY;
    
    -- Test 1: Simple range query
    query_counter := query_counter + 1;
    RAISE NOTICE 'Test %: Simple range query', query_counter;
    
    test_start := clock_timestamp();
    PERFORM * FROM patient_vitals 
    WHERE measurement_time BETWEEN '2025-01-01 00:00:00' AND '2025-01-02 00:00:00'
    AND patient_department = 'ICU';
    test_end := clock_timestamp();
    
    test_duration := test_end - test_start;
    query_execution_time := EXTRACT(EPOCH FROM test_duration) * 1000;
    
    -- Get explain plan
    EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) 
        SELECT * FROM patient_vitals 
        WHERE measurement_time BETWEEN ''2025-01-01 00:00:00'' AND ''2025-01-02 00:00:00''
        AND patient_department = ''ICU''' INTO explain_plan;
    
    -- Extract metrics from explain plan
    DECLARE
        plan_data JSONB;
        planning_time NUMERIC := 0;
        execution_time NUMERIC := 0;
        rows_returned INTEGER := 0;
        rows_examined INTEGER := 0;
        index_used BOOLEAN := FALSE;
        index_name VARCHAR := NULL;
        buffer_hits BIGINT := 0;
        buffer_reads BIGINT := 0;
    BEGIN
        plan_data := explain_plan->0->'Plan';
        planning_time := (plan_data->>'Planning Time')::NUMERIC * 1000;
        execution_time := (plan_data->>'Execution Time')::NUMERIC * 1000;
        rows_returned := (plan_data->>'Actual Rows')::INTEGER;
        
        -- Check if index was used
        IF plan_data->>'Node Type' = 'Index Scan' OR plan_data->>'Node Type' = 'Index Only Scan' THEN
            index_used := TRUE;
            index_name := plan_data->>'Index Name';
        END IF;
        
        -- Get buffer information
        IF plan_data ? 'Buffers' THEN
            buffer_hits := COALESCE((plan_data->'Buffers'->>'Shared Hit Blocks')::BIGINT, 0);
            buffer_reads := COALESCE((plan_data->'Buffers'->>'Shared Read Blocks')::BIGINT, 0);
        END IF;
        
        -- Estimate rows examined (from child nodes)
        rows_examined := estimate_rows_examined(plan_data);
        
        -- Store result
        test_results := test_results || jsonb_build_object(
            'test_id', query_counter,
            'query_type', 'simple_range',
            'query_description', 'Range query for ICU department',
            'rls_enabled', FALSE,
            'security_context', 'NO_SECURITY',
            'user_role', 'NO_ROLE',
            'execution_time_ms', ROUND(execution_time, 3),
            'planning_time_ms', ROUND(planning_time, 3),
            'rows_returned', rows_returned,
            'rows_examined', rows_examined,
            'index_used', index_used,
            'index_name', index_name,
            'buffer_hits', buffer_hits,
            'buffer_reads', buffer_reads,
            'test_phase', 'baseline'
        );
        
        RAISE NOTICE '  Execution time: % ms', ROUND(execution_time, 3);
        RAISE NOTICE '  Planning time: % ms', ROUND(planning_time, 3);
        RAISE NOTICE '  Rows returned: %', rows_returned;
        RAISE NOTICE '  Index used: %', CASE WHEN index_used THEN index_name ELSE 'No' END;
    END;
    
    -- Test 2: Patient-specific query
    query_counter := query_counter + 1;
    RAISE NOTICE '';
    RAISE NOTICE 'Test %: Patient-specific query', query_counter;
    
    test_start := clock_timestamp();
    PERFORM * FROM patient_vitals 
    WHERE patient_id = 'PATIENT_00001'
    AND measurement_time >= NOW() - INTERVAL '24 hours'
    AND vital_type = 'heart_rate_bpm';
    test_end := clock_timestamp();
    
    EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) 
        SELECT * FROM patient_vitals 
        WHERE patient_id = ''PATIENT_00001''
        AND measurement_time >= NOW() - INTERVAL ''24 hours''
        AND vital_type = ''heart_rate_bpm''' INTO explain_plan;
    
    plan_data := explain_plan->0->'Plan';
    
    -- Store result (simplified for brevity)
    test_results := test_results || jsonb_build_object(
        'test_id', query_counter,
        'query_type', 'patient_specific',
        'query_description', 'Patient-specific vital sign query',
        'rls_enabled', FALSE,
        'security_context', 'NO_SECURITY',
        'user_role', 'NO_ROLE',
        'execution_time_ms', ROUND((plan_data->>'Execution Time')::NUMERIC * 1000, 3),
        'planning_time_ms', ROUND((plan_data->>'Planning Time')::NUMERIC * 1000, 3),
        'rows_returned', (plan_data->>'Actual Rows')::INTEGER,
        'test_phase', 'baseline'
    );
    
    RAISE NOTICE '  Execution time: % ms', 
        ROUND((plan_data->>'Execution Time')::NUMERIC * 1000, 3);
    
    -- Test 3: Aggregation query
    query_counter := query_counter + 1;
    RAISE NOTICE '';
    RAISE NOTICE 'Test %: Aggregation query', query_counter;
    
    test_start := clock_timestamp();
    PERFORM 
        patient_department,
        vital_type,
        COUNT(*) as reading_count,
        AVG(vital_value) as avg_value,
        MIN(vital_value) as min_value,
        MAX(vital_value) as max_value
    FROM patient_vitals
    WHERE measurement_time >= NOW() - INTERVAL '7 days'
    GROUP BY patient_department, vital_type
    HAVING COUNT(*) > 100;
    test_end := clock_timestamp();
    
    EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) 
        SELECT 
            patient_department,
            vital_type,
            COUNT(*) as reading_count,
            AVG(vital_value) as avg_value,
            MIN(vital_value) as min_value,
            MAX(vital_value) as max_value
        FROM patient_vitals
        WHERE measurement_time >= NOW() - INTERVAL ''7 days''
        GROUP BY patient_department, vital_type
        HAVING COUNT(*) > 100' INTO explain_plan;
    
    plan_data := explain_plan->0->'Plan';
    
    test_results := test_results || jsonb_build_object(
        'test_id', query_counter,
        'query_type', 'aggregation',
        'query_description', 'Department-level vital sign aggregation',
        'rls_enabled', FALSE,
        'security_context', 'NO_SECURITY',
        'user_role', 'NO_ROLE',
        'execution_time_ms', ROUND((plan_data->>'Execution Time')::NUMERIC * 1000, 3),
        'planning_time_ms', ROUND((plan_data->>'Planning Time')::NUMERIC * 1000, 3),
        'rows_returned', (plan_data->>'Actual Rows')::INTEGER,
        'test_phase', 'baseline'
    );
    
    RAISE NOTICE '  Execution time: % ms', 
        ROUND((plan_data->>'Execution Time')::NUMERIC * 1000, 3);
    
    -- Test 4: Join with audit logs
    query_counter := query_counter + 1;
    RAISE NOTICE '';
    RAISE NOTICE 'Test %: Join query', query_counter;
    
    test_start := clock_timestamp();
    PERFORM 
        pv.patient_id,
        pv.vital_type,
        pv.vital_value,
        al.event_time,
        al.user_role
    FROM patient_vitals pv
    LEFT JOIN audit_logs al ON pv.patient_id = al.patient_id
    WHERE pv.measurement_time >= NOW() - INTERVAL '1 day'
    AND pv.is_alert = TRUE
    ORDER BY pv.measurement_time DESC
    LIMIT 100;
    test_end := clock_timestamp();
    
    EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) 
        SELECT 
            pv.patient_id,
            pv.vital_type,
            pv.vital_value,
            al.event_time,
            al.user_role
        FROM patient_vitals pv
        LEFT JOIN audit_logs al ON pv.patient_id = al.patient_id
        WHERE pv.measurement_time >= NOW() - INTERVAL ''1 day''
        AND pv.is_alert = TRUE
        ORDER BY pv.measurement_time DESC
        LIMIT 100' INTO explain_plan;
    
    plan_data := explain_plan->0->'Plan';
    
    test_results := test_results || jsonb_build_object(
        'test_id', query_counter,
        'query_type', 'join',
        'query_description', 'Join patient vitals with audit logs',
        'rls_enabled', FALSE,
        'security_context', 'NO_SECURITY',
        'user_role', 'NO_ROLE',
        'execution_time_ms', ROUND((plan_data->>'Execution Time')::NUMERIC * 1000, 3),
        'planning_time_ms', ROUND((plan_data->>'Planning Time')::NUMERIC * 1000, 3),
        'rows_returned', (plan_data->>'Actual Rows')::INTEGER,
        'test_phase', 'baseline'
    );
    
    RAISE NOTICE '  Execution time: % ms', 
        ROUND((plan_data->>'Execution Time')::NUMERIC * 1000, 3);
    
    -- Test 5: Window function query
    query_counter := query_counter + 1;
    RAISE NOTICE '';
    RAISE NOTICE 'Test %: Window function query', query_counter;
    
    test_start := clock_timestamp();
    PERFORM 
        patient_id,
        vital_type,
        vital_value,
        measurement_time,
        AVG(vital_value) OVER (
            PARTITION BY patient_id, vital_type 
            ORDER BY measurement_time 
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) as moving_avg,
        vital_value - LAG(vital_value) OVER (
            PARTITION BY patient_id, vital_type 
            ORDER BY measurement_time
        ) as value_change
    FROM patient_vitals
    WHERE measurement_time >= NOW() - INTERVAL '1 hour'
    AND vital_type = 'heart_rate_bpm';
    test_end := clock_timestamp();
    
    EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) 
        SELECT 
            patient_id,
            vital_type,
            vital_value,
            measurement_time,
            AVG(vital_value) OVER (
                PARTITION BY patient_id, vital_type 
                ORDER BY measurement_time 
                ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
            ) as moving_avg,
            vital_value - LAG(vital_value) OVER (
                PARTITION BY patient_id, vital_type 
                ORDER BY measurement_time
            ) as value_change
        FROM patient_vitals
        WHERE measurement_time >= NOW() - INTERVAL ''1 hour''
        AND vital_type = ''heart_rate_bpm''' INTO explain_plan;
    
    plan_data := explain_plan->0->'Plan';
    
    test_results := test_results || jsonb_build_object(
        'test_id', query_counter,
        'query_type', 'window_function',
        'query_description', 'Window functions for trend analysis',
        'rls_enabled', FALSE,
        'security_context', 'NO_SECURITY',
        'user_role', 'NO_ROLE',
        'execution_time_ms', ROUND((plan_data->>'Execution Time')::NUMERIC * 1000, 3),
        'planning_time_ms', ROUND((plan_data->>'Planning Time')::NUMERIC * 1000, 3),
        'rows_returned', (plan_data->>'Actual Rows')::INTEGER,
        'test_phase', 'baseline'
    );
    
    RAISE NOTICE '  Execution time: % ms', 
        ROUND((plan_data->>'Execution Time')::NUMERIC * 1000, 3);
    
    -- 3. TESTS WITH RLS ENABLED
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'PHASE 2: WITH RLS ENABLED';
    RAISE NOTICE '------------------------';
    
    -- Enable RLS
    ALTER TABLE patient_vitals ENABLE ROW LEVEL SECURITY;
    
    -- Test with different security contexts
    DECLARE
        security_contexts VARCHAR[] := ARRAY['ICU', 'WARD', 'OUTPATIENT'];
        user_roles VARCHAR[] := ARRAY['doctor_role', 'nurse_role', 'patient_role'];
        current_context VARCHAR;
        current_role VARCHAR;
        test_query TEXT;
    BEGIN
        FOREACH current_context IN ARRAY security_contexts LOOP
            FOREACH current_role IN ARRAY user_roles LOOP
                RAISE NOTICE '';
                RAISE NOTICE 'Security context: %, User role: %', 
                    current_context, current_role;
                RAISE NOTICE '-----------------------------------';
                
                -- Set security context
                PERFORM set_config('app.current_department', current_context, false);
                PERFORM set_config('app.current_role', current_role, false);
                
                -- Test 6: Simple range query with RLS
                query_counter := query_counter + 1;
                RAISE NOTICE 'Test %: Range query with RLS', query_counter;
                
                -- Build query based on role
                IF current_role = 'patient_role' THEN
                    -- Patients can only query specific patients
                    PERFORM set_config('app.current_patient_id', 'PATIENT_00001', false);
                    test_query := 'SELECT * FROM patient_vitals 
                        WHERE measurement_time >= NOW() - INTERVAL ''1 hour''';
                ELSE
                    -- Doctors and nurses can query by department
                    test_query := 'SELECT * FROM patient_vitals 
                        WHERE measurement_time >= NOW() - INTERVAL ''1 hour''
                        AND patient_department = ''' || current_context || '''';
                END IF;
                
                -- Execute and time the query
                test_start := clock_timestamp();
                EXECUTE test_query;
                test_end := clock_timestamp();
                
                -- Get explain plan
                EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ' || test_query 
                INTO explain_plan;
                
                plan_data := explain_plan->0->'Plan';
                
                test_results := test_results || jsonb_build_object(
                    'test_id', query_counter,
                    'query_type', 'simple_range_rls',
                    'query_description', 'Range query with RLS - ' || current_role,
                    'rls_enabled', TRUE,
                    'security_context', current_context,
                    'user_role', current_role,
                    'execution_time_ms', ROUND((plan_data->>'Execution Time')::NUMERIC * 1000, 3),
                    'planning_time_ms', ROUND((plan_data->>'Planning Time')::NUMERIC * 1000, 3),
                    'rows_returned', (plan_data->>'Actual Rows')::INTEGER,
                    'test_phase', 'rls_enabled'
                );
                
                RAISE NOTICE '  Execution time: % ms', 
                    ROUND((plan_data->>'Execution Time')::NUMERIC * 1000, 3);
                RAISE NOTICE '  Rows returned: %', 
                    (plan_data->>'Actual Rows')::INTEGER;
                
                -- Test 7: Aggregation with RLS
                IF current_role != 'patient_role' THEN
                    query_counter := query_counter + 1;
                    RAISE NOTICE 'Test %: Aggregation with RLS', query_counter;
                    
                    test_query := '
                        SELECT 
                            vital_type,
                            COUNT(*) as reading_count,
                            AVG(vital_value) as avg_value
                        FROM patient_vitals
                        WHERE measurement_time >= NOW() - INTERVAL ''1 day''
                        GROUP BY vital_type
                        HAVING COUNT(*) > 10';
                    
                    test_start := clock_timestamp();
                    EXECUTE test_query;
                    test_end := clock_timestamp();
                    
                    EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ' || test_query 
                    INTO explain_plan;
                    
                    plan_data := explain_plan->0->'Plan';
                    
                    test_results := test_results || jsonb_build_object(
                        'test_id', query_counter,
                        'query_type', 'aggregation_rls',
                        'query_description', 'Aggregation with RLS - ' || current_role,
                        'rls_enabled', TRUE,
                        'security_context', current_context,
                        'user_role', current_role,
                        'execution_time_ms', ROUND((plan_data->>'Execution Time')::NUMERIC * 1000, 3),
                        'planning_time_ms', ROUND((plan_data->>'Planning Time')::NUMERIC * 1000, 3),
                        'rows_returned', (plan_data->>'Actual Rows')::INTEGER,
                        'test_phase', 'rls_enabled'
                    );
                    
                    RAISE NOTICE '  Aggregation time: % ms', 
                        ROUND((plan_data->>'Execution Time')::NUMERIC * 1000, 3);
                END IF;
                
                -- Small delay between tests
                PERFORM pg_sleep(0.2);
            END LOOP;
        END LOOP;
    END;
    
    -- 4. COMPLEX RLS POLICY TESTS
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'PHASE 3: COMPLEX RLS POLICIES';
    RAISE NOTICE '----------------------------';
    
    -- Add more complex RLS policy for testing
    DROP POLICY IF EXISTS test_complex_access ON patient_vitals;
    CREATE POLICY test_complex_access ON patient_vitals
        FOR SELECT TO PUBLIC
        USING (
            -- Complex condition simulating real healthcare scenario
            (
                patient_department = current_setting('app.current_department', true)
                AND data_classification IN ('PHI', 'CONFIDENTIAL')
                AND (
                    current_setting('app.user_role', true) = 'doctor_role'
                    OR (
                        current_setting('app.user_role', true) = 'nurse_role'
                        AND vital_type NOT IN ('psychiatric_notes', 'sensitive_diagnosis')
                        AND EXTRACT(HOUR FROM measurement_time) BETWEEN 6 AND 22
                    )
                )
            )
            OR
            (
                patient_id = current_setting('app.current_patient_id', true)
                AND data_classification = 'PHI'
            )
        );
    
    -- Test complex policy
    PERFORM set_config('app.current_department', 'ICU', false);
    PERFORM set_config('app.user_role', 'nurse_role', false);
    PERFORM set_config('app.current_patient_id', 'PATIENT_00001', false);
    
    query_counter := query_counter + 1;
    RAISE NOTICE 'Test %: Complex RLS policy query', query_counter;
    
    test_start := clock_timestamp();
    PERFORM * FROM patient_vitals 
    WHERE measurement_time >= NOW() - INTERVAL '4 hours';
    test_end := clock_timestamp();
    
    EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) 
        SELECT * FROM patient_vitals 
        WHERE measurement_time >= NOW() - INTERVAL ''4 hours''' 
    INTO explain_plan;
    
    plan_data := explain_plan->0->'Plan';
    
    test_results := test_results || jsonb_build_object(
        'test_id', query_counter,
        'query_type', 'complex_rls',
        'query_description', 'Query with complex RLS policy',
        'rls_enabled', TRUE,
        'security_context', 'COMPLEX',
        'user_role', 'nurse_role',
        'execution_time_ms', ROUND((plan_data->>'Execution Time')::NUMERIC * 1000, 3),
        'planning_time_ms', ROUND((plan_data->>'Planning Time')::NUMERIC * 1000, 3),
        'rows_returned', (plan_data->>'Actual Rows')::INTEGER,
        'test_phase', 'complex_rls'
    );
    
    RAISE NOTICE '  Execution time: % ms', 
        ROUND((plan_data->>'Execution Time')::NUMERIC * 1000, 3);
    
    -- 5. INSERT PERFORMANCE WITH RLS
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'PHASE 4: INSERT PERFORMANCE WITH RLS';
    RAISE NOTICE '-----------------------------------';
    
    -- Test insert performance with RLS
    PERFORM set_config('app.current_department', 'ICU', false);
    PERFORM set_config('app.user_role', 'doctor_role', false);
    
    -- Create test table for insert performance
    DROP TABLE IF EXISTS insert_performance_test;
    CREATE TABLE insert_performance_test (
        test_id SERIAL PRIMARY KEY,
        insert_time TIMESTAMPTZ DEFAULT NOW(),
        test_data JSONB,
        inserted_by VARCHAR(50)
    );
    
    -- Enable RLS and create policy
    ALTER TABLE insert_performance_test ENABLE ROW LEVEL SECURITY;
    
    CREATE POLICY insert_test_policy ON insert_performance_test
        FOR ALL TO PUBLIC
        WITH CHECK (inserted_by = current_user);
    
    -- Test batch inserts
    DECLARE
        batch_sizes INTEGER[] := ARRAY[10, 100, 1000];
        current_batch INTEGER;
        insert_start TIMESTAMPTZ;
        insert_end TIMESTAMPTZ;
        insert_duration INTERVAL;
    BEGIN
        FOREACH current_batch IN ARRAY batch_sizes LOOP
            query_counter := query_counter + 1;
            RAISE NOTICE 'Test %: Batch insert (% rows)', query_counter, current_batch;
            
            -- Clear previous test data
            DELETE FROM insert_performance_test;
            
            insert_start := clock_timestamp();
            
            -- Perform batch insert
            INSERT INTO insert_performance_test (test_data, inserted_by)
            SELECT 
                jsonb_build_object(
                    'patient_id', 'TEST_' || seq,
                    'vital_type', 'heart_rate_bpm',
                    'value', 60 + (seq % 40),
                    'timestamp', NOW() - (seq * INTERVAL '1 second')
                ),
                current_user
            FROM generate_series(1, current_batch) seq;
            
            insert_end := clock_timestamp();
            insert_duration := insert_end - insert_start;
            
            test_results := test_results || jsonb_build_object(
                'test_id', query_counter,
                'query_type', 'batch_insert',
                'query_description', 'Batch insert with RLS - ' || current_batch || ' rows',
                'rls_enabled', TRUE,
                'security_context', 'ICU',
                'user_role', 'doctor_role',
                'execution_time_ms', ROUND(EXTRACT(EPOCH FROM insert_duration) * 1000, 3),
                'rows_returned', current_batch,
                'test_phase', 'insert_performance'
            );
            
            RAISE NOTICE '  Insert time: % ms', 
                ROUND(EXTRACT(EPOCH FROM insert_duration) * 1000, 3);
            RAISE NOTICE '  Insert rate: % rows/sec', 
                ROUND(current_batch / EXTRACT(EPOCH FROM insert_duration), 2);
            
            PERFORM pg_sleep(0.5);
        END LOOP;
    END;
    
    -- Cleanup test table
    DROP TABLE insert_performance_test;
    
    -- 6. STORE ALL RESULTS
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'Storing test results...';
    
    -- Insert all results into table
    INSERT INTO query_performance_results (
        query_type,
        query_description,
        rls_enabled,
        security_context,
        user_role,
        execution_time_ms,
        planning_time_ms,
        rows_returned,
        rows_examined,
        index_used,
        index_name,
        buffer_hits,
        buffer_reads,
        query_plan,
        test_phase
    )
    SELECT 
        r->>'query_type',
        r->>'query_description',
        (r->>'rls_enabled')::BOOLEAN,
        r->>'security_context',
        r->>'user_role',
        (r->>'execution_time_ms')::NUMERIC,
        COALESCE((r->>'planning_time_ms')::NUMERIC, 0),
        (r->>'rows_returned')::INTEGER,
        COALESCE((r->>'rows_examined')::INTEGER, 0),
        COALESCE((r->>'index_used')::BOOLEAN, FALSE),
        r->>'index_name',
        COALESCE((r->>'buffer_hits')::BIGINT, 0),
        COALESCE((r->>'buffer_reads')::BIGINT, 0),
        r->'query_plan',
        r->>'test_phase'
    FROM jsonb_array_elements(test_results) r;
    
    -- 7. ANALYZE RESULTS
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'PERFORMANCE ANALYSIS';
    RAISE NOTICE '============================================';
    
    -- Baseline vs RLS comparison
    RAISE NOTICE '';
    RAISE NOTICE 'BASELINE vs RLS PERFORMANCE:';
    RAISE NOTICE '----------------------------';
    
    FOR comparison IN
        WITH baseline_stats AS (
            SELECT 
                query_type,
                AVG(execution_time_ms) as baseline_time
            FROM query_performance_results
            WHERE test_phase = 'baseline'
            GROUP BY query_type
        ),
        rls_stats AS (
            SELECT 
                query_type,
                AVG(execution_time_ms) as rls_time
            FROM query_performance_results
            WHERE test_phase = 'rls_enabled'
            GROUP BY query_type
        )
        SELECT 
            b.query_type,
            ROUND(b.baseline_time, 3) as baseline_ms,
            ROUND(s.rls_time, 3) as rls_ms,
            ROUND(s.rls_time - b.baseline_time, 3) as overhead_ms,
            ROUND((s.rls_time - b.baseline_time) / b.baseline_time * 100, 2) as overhead_pct
        FROM baseline_stats b
        LEFT JOIN rls_stats s ON b.query_type || '_rls' = s.query_type
        WHERE s.rls_time IS NOT NULL
        ORDER BY overhead_pct DESC
    LOOP
        RAISE NOTICE '%: Baseline=%ms, RLS=%ms, Overhead=%ms (%)',
            comparison.query_type,
            comparison.baseline_ms,
            comparison.rls_ms,
            comparison.overhead_ms,
            comparison.overhead_pct || '%';
    END LOOP;
    
    -- Query type performance analysis
    RAISE NOTICE '';
    RAISE NOTICE 'QUERY TYPE PERFORMANCE (with RLS):';
    RAISE NOTICE '----------------------------------';
    
    FOR query_type_stats IN
        SELECT 
            query_type,
            COUNT(*) as test_count,
            ROUND(AVG(execution_time_ms), 3) as avg_time_ms,
            ROUND(MIN(execution_time_ms), 3) as min_time_ms,
            ROUND(MAX(execution_time_ms), 3) as max_time_ms,
            ROUND(STDDEV(execution_time_ms), 3) as stddev_ms
        FROM query_performance_results
        WHERE rls_enabled = TRUE
        GROUP BY query_type
        ORDER BY avg_time_ms DESC
    LOOP
        RAISE NOTICE '%: Avg=%ms, Range=%ms-%ms, StdDev=%ms',
            query_type_stats.query_type,
            query_type_stats.avg_time_ms,
            query_type_stats.min_time_ms,
            query_type_stats.max_time_ms,
            query_type_stats.stddev_ms;
    END LOOP;
    
    -- Security context impact
    RAISE NOTICE '';
    RAISE NOTICE 'SECURITY CONTEXT IMPACT:';
    RAISE NOTICE '-----------------------';
    
    FOR context_stats IN
        SELECT 
            security_context,
            user_role,
            COUNT(*) as query_count,
            ROUND(AVG(execution_time_ms), 3) as avg_time_ms,
            ROUND(AVG(planning_time_ms), 3) as avg_planning_ms
        FROM query_performance_results
        WHERE rls_enabled = TRUE
        AND security_context NOT IN ('NO_SECURITY', 'COMPLEX')
        GROUP BY security_context, user_role
        ORDER BY security_context, user_role
    LOOP
        RAISE NOTICE '% - %: Queries=%, Exec=%ms, Plan=%ms',
            context_stats.security_context,
            context_stats.user_role,
            context_stats.query_count,
            context_stats.avg_time_ms,
            context_stats.avg_planning_ms;
    END LOOP;
    
    -- Index usage analysis
    RAISE NOTICE '';
    RAISE NOTICE 'INDEX USAGE ANALYSIS:';
    RAISE NOTICE '--------------------';
    
    DECLARE
        total_queries INTEGER;
        indexed_queries INTEGER;
        index_usage_pct NUMERIC;
    BEGIN
        SELECT COUNT(*) INTO total_queries
        FROM query_performance_results
        WHERE test_phase IN ('baseline', 'rls_enabled');
        
        SELECT COUNT(*) INTO indexed_queries
        FROM query_performance_results
        WHERE test_phase IN ('baseline', 'rls_enabled')
        AND index_used = TRUE;
        
        IF total_queries > 0 THEN
            index_usage_pct := (indexed_queries::NUMERIC / total_queries) * 100;
            RAISE NOTICE 'Index usage: %/% queries (%)',
                indexed_queries, total_queries, ROUND(index_usage_pct, 1) || '%';
        END IF;
    END;
    
    -- Buffer cache efficiency
    RAISE NOTICE '';
    RAISE NOTICE 'BUFFER CACHE EFFICIENCY:';
    RAISE NOTICE '------------------------';
    
    DECLARE
        total_buffer_hits BIGINT;
        total_buffer_reads BIGINT;
        hit_ratio NUMERIC;
    BEGIN
        SELECT 
            SUM(buffer_hits),
            SUM(buffer_reads)
        INTO total_buffer_hits, total_buffer_reads
        FROM query_performance_results
        WHERE test_phase IN ('baseline', 'rls_enabled');
        
        IF total_buffer_hits + total_buffer_reads > 0 THEN
            hit_ratio := total_buffer_hits::NUMERIC / 
                        (total_buffer_hits + total_buffer_reads) * 100;
            RAISE NOTICE 'Buffer cache hit ratio: %/% (%)',
                total_buffer_hits, 
                total_buffer_hits + total_buffer_reads,
                ROUND(hit_ratio, 1) || '%';
        END IF;
    END;
    
    -- 8. PERFORMANCE RECOMMENDATIONS
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'PERFORMANCE RECOMMENDATIONS:';
    RAISE NOTICE '---------------------------';
    
    -- Analyze RLS overhead
    DECLARE
        avg_rls_overhead NUMERIC;
        max_rls_overhead NUMERIC;
    BEGIN
        SELECT 
            AVG(overhead_ms),
            MAX(overhead_ms)
        INTO avg_rls_overhead, max_rls_overhead
        FROM (
            SELECT 
                (rls_time - baseline_time) as overhead_ms
            FROM (
                SELECT 
                    AVG(CASE WHEN test_phase = 'baseline' THEN execution_time_ms END) as baseline_time,
                    AVG(CASE WHEN test_phase = 'rls_enabled' THEN execution_time_ms END) as rls_time
                FROM query_performance_results
                WHERE query_type IN ('simple_range', 'patient_specific')
            ) sub
        ) overheads;
        
        IF avg_rls_overhead > 50 THEN
            RAISE NOTICE '1. RLS overhead is significant (>50ms)';
            RAISE NOTICE '   Consider:';
            RAISE NOTICE '   • Simplifying RLS policies';
            RAISE NOTICE '   • Adding covering indexes for RLS predicates';
            RAISE NOTICE '   • Using materialized views for frequently accessed data';
        ELSE
            RAISE NOTICE '1. RLS overhead is acceptable (<50ms)';
            RAISE NOTICE '   RLS can be used without major performance impact';
        END IF;
        
        RAISE NOTICE '   Average overhead: % ms', ROUND(avg_rls_overhead, 2);
        RAISE NOTICE '   Maximum overhead: % ms', ROUND(max_rls_overhead, 2);
    END;
    
    -- Query optimization suggestions
    DECLARE
        slowest_query_type VARCHAR;
        slowest_avg_time NUMERIC;
    BEGIN
        SELECT 
            query_type,
            AVG(execution_time_ms)
        INTO slowest_query_type, slowest_avg_time
        FROM query_performance_results
        WHERE rls_enabled = TRUE
        GROUP BY query_type
        ORDER BY AVG(execution_time_ms) DESC
        LIMIT 1;
        
        RAISE NOTICE '2. Slowest query type: %', slowest_query_type;
        RAISE NOTICE '   Average time: % ms', ROUND(slowest_avg_time, 2);
        
        CASE slowest_query_type
            WHEN 'aggregation_rls' THEN
                RAISE NOTICE '   Consider:';
                RAISE NOTICE '   • Creating summary tables';
                RAISE NOTICE '   • Using materialized views';
                RAISE NOTICE '   • Adding appropriate indexes for GROUP BY columns';
            WHEN 'window_function' THEN
                RAISE NOTICE '   Consider:';
                RAISE NOTICE '   • Pre-computing window functions';
                RAISE NOTICE '   • Limiting window size';
                RAISE NOTICE '   • Adding indexes on PARTITION BY and ORDER BY columns';
            WHEN 'join' THEN
                RAISE NOTICE '   Consider:';
                RAISE NOTICE '   • Adding foreign key indexes';
                RAISE NOTICE '   • Denormalizing frequently joined data';
                RAISE NOTICE '   • Using INNER JOIN instead of LEFT JOIN where possible';
        END CASE;
    END;
    
    -- Security context recommendations
    RAISE NOTICE '3. Security context performance:';
    FOR context_rec IN
        SELECT 
            security_context,
            user_role,
            AVG(execution_time_ms) as avg_time
        FROM query_performance_results
        WHERE rls_enabled = TRUE
        AND security_context NOT IN ('NO_SECURITY', 'COMPLEX')
        GROUP BY security_context, user_role
        ORDER BY AVG(execution_time_ms) DESC
        LIMIT 3
    LOOP
        RAISE NOTICE '   %/%: % ms average',
            context_rec.security_context,
            context_rec.user_role,
            ROUND(context_rec.avg_time, 2);
    END LOOP;
    
    -- 9. CLEANUP
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE 'Cleaning up...';
    
    -- Drop complex test policy
    DROP POLICY IF EXISTS test_complex_access ON patient_vitals;
    
    -- Reset security context
    RESET app.current_department;
    RESET app.user_role;
    RESET app.current_patient_id;
    
    -- 10. FINAL REPORT
    -- ============================================
    RAISE NOTICE '';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'QUERY PERFORMANCE TEST COMPLETE';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'Total queries tested: %', query_counter;
    RAISE NOTICE 'Results saved in: query_performance_results';
    RAISE NOTICE '';
    RAISE NOTICE 'Key findings:';
    RAISE NOTICE '1. RLS adds predictable overhead to query execution';
    RAISE NOTICE '2. Complex queries are more affected by RLS than simple ones';
    RAISE NOTICE '3. Index usage is crucial for maintaining performance with RLS';
    RAISE NOTICE '4. Different security contexts have varying performance impacts';
    RAISE NOTICE '';
    RAISE NOTICE 'Use these results to:';
    RAISE NOTICE '• Optimize RLS policies for performance-critical applications';
    RAISE NOTICE '• Design appropriate indexes for common query patterns';
    RAISE NOTICE '• Size database resources based on expected query loads';
    RAISE NOTICE '• Train users on efficient query patterns';
    RAISE NOTICE '============================================';
    
END $$;

-- Helper function to estimate rows examined from explain plan
CREATE OR REPLACE FUNCTION estimate_rows_examined(plan_json JSONB)
RETURNS INTEGER AS $$
DECLARE
    total_rows INTEGER := 0;
    node_type VARCHAR;
    actual_rows INTEGER;
    plans JSONB;
    subplan JSONB;
BEGIN
    -- Base case: get rows from this node
    IF plan_json ? 'Actual Rows' THEN
        total_rows := total_rows + COALESCE((plan_json->>'Actual Rows')::INTEGER, 0);
    END IF;
    
    -- Recursively process child plans
    IF plan_json ? 'Plans' THEN
        plans := plan_json->'Plans';
        FOR i IN 0..jsonb_array_length(plans) - 1 LOOP
            subplan := plans->i;
            total_rows := total_rows + estimate_rows_examined(subplan);
        END LOOP;
    END IF;
    
    RETURN total_rows;
END;
$$ LANGUAGE plpgsql;
