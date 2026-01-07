-- Database Comparison Analysis
-- Generates comprehensive comparison tables and analysis

-- Create comparison schema
CREATE SCHEMA IF NOT EXISTS benchmark_comparison;

-- Table to store database comparison results
CREATE TABLE IF NOT EXISTS benchmark_comparison.database_comparison (
    comparison_id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    database_name VARCHAR(50) NOT NULL,
    test_category VARCHAR(50) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC(15,4),
    unit VARCHAR(20),
    weight NUMERIC(5,3) DEFAULT 1.0,
    normalized_score NUMERIC(10,4),
    notes TEXT
);

-- Table for final scores
CREATE TABLE IF NOT EXISTS benchmark_comparison.final_scores (
    score_id SERIAL PRIMARY KEY,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    database_name VARCHAR(50) NOT NULL,
    ingestion_score NUMERIC(10,4),
    query_score NUMERIC(10,4),
    storage_score NUMERIC(10,4),
    indexing_score NUMERIC(10,4),
    security_score NUMERIC(10,4),
    total_score NUMERIC(10,4),
    ranking INTEGER,
    performance_category VARCHAR(20)
);

-- Function to calculate normalized scores
CREATE OR REPLACE FUNCTION benchmark_comparison.calculate_normalized_scores(
    p_run_timestamp TIMESTAMP DEFAULT NULL
)
RETURNS TABLE (
    database_name VARCHAR(50),
    test_category VARCHAR(50),
    metric_name VARCHAR(100),
    original_value NUMERIC,
    normalized_score NUMERIC,
    min_value NUMERIC,
    max_value NUMERIC,
    is_higher_better BOOLEAN
) AS $$
BEGIN
    RETURN QUERY
    WITH metric_info AS (
        SELECT 
            metric_name,
            CASE 
                WHEN metric_name LIKE '%latency%' OR metric_name LIKE '%time%' THEN FALSE
                WHEN metric_name LIKE '%rate%' OR metric_name LIKE '%throughput%' THEN TRUE
                WHEN metric_name LIKE '%efficiency%' THEN TRUE
                WHEN metric_name LIKE '%score%' THEN TRUE
                WHEN metric_name LIKE '%size%' THEN FALSE
                ELSE TRUE
            END AS higher_is_better
        FROM benchmark_comparison.database_comparison
        GROUP BY metric_name
    ),
    metric_stats AS (
        SELECT 
            dc.metric_name,
            MIN(dc.metric_value) as min_val,
            MAX(dc.metric_value) as max_val,
            mi.higher_is_better
        FROM benchmark_comparison.database_comparison dc
        JOIN metric_info mi ON dc.metric_name = mi.metric_name
        WHERE (p_run_timestamp IS NULL OR dc.run_timestamp = p_run_timestamp)
        GROUP BY dc.metric_name, mi.higher_is_better
    )
    SELECT 
        dc.database_name,
        dc.test_category,
        dc.metric_name,
        dc.metric_value as original_value,
        CASE 
            WHEN ms.higher_is_better THEN
                CASE 
                    WHEN ms.max_val - ms.min_val > 0 THEN
                        (dc.metric_value - ms.min_val) / (ms.max_val - ms.min_val) * 100
                    ELSE 100
                END
            ELSE
                CASE 
                    WHEN ms.max_val - ms.min_val > 0 THEN
                        (ms.max_val - dc.metric_value) / (ms.max_val - ms.min_val) * 100
                    ELSE 100
                END
        END as normalized_score,
        ms.min_val,
        ms.max_val,
        ms.higher_is_better
    FROM benchmark_comparison.database_comparison dc
    JOIN metric_stats ms ON dc.metric_name = ms.metric_name
    WHERE (p_run_timestamp IS NULL OR dc.run_timestamp = p_run_timestamp);
END;
$$ LANGUAGE plpgsql;

-- Procedure to populate comparison data from benchmark results
CREATE OR REPLACE PROCEDURE benchmark_comparison.populate_comparison_data()
LANGUAGE plpgsql
AS $$
DECLARE
    v_run_timestamp TIMESTAMP := CURRENT_TIMESTAMP;
BEGIN
    -- Clear existing data for this run
    DELETE FROM benchmark_comparison.database_comparison 
    WHERE run_timestamp = v_run_timestamp;
    
    -- Insert PostgreSQL metrics
    INSERT INTO benchmark_comparison.database_comparison 
    (run_timestamp, database_name, test_category, metric_name, metric_value, unit, weight)
    VALUES 
    -- Ingestion Performance
    (v_run_timestamp, 'PostgreSQL', 'ingestion', 'insert_rate', 
        (SELECT COALESCE(AVG(inserts_per_second), 0) FROM benchmark_results.ingestion_performance), 'rows/sec', 0.25),
    
    (v_run_timestamp, 'PostgreSQL', 'ingestion', 'batch_insert_rate',
        (SELECT COALESCE(AVG(batch_insert_rate), 0) FROM benchmark_results.ingestion_performance), 'rows/sec', 0.20),
    
    -- Query Performance
    (v_run_timestamp, 'PostgreSQL', 'query', 'simple_query_latency',
        (SELECT COALESCE(AVG(execution_time_ms), 0) FROM benchmark_results.query_performance 
         WHERE query_type = 'simple_select'), 'ms', 0.15),
    
    (v_run_timestamp, 'PostgreSQL', 'query', 'complex_query_latency',
        (SELECT COALESCE(AVG(execution_time_ms), 0) FROM benchmark_results.query_performance 
         WHERE query_type = 'complex_join'), 'ms', 0.15),
    
    -- Storage Efficiency
    (v_run_timestamp, 'PostgreSQL', 'storage', 'table_size',
        (SELECT COALESCE(SUM(total_bytes)/1024/1024, 0) FROM benchmark_results.storage_stats), 'MB', 0.10),
    
    (v_run_timestamp, 'PostgreSQL', 'storage', 'index_size',
        (SELECT COALESCE(SUM(index_bytes)/1024/1024, 0) FROM benchmark_results.storage_stats), 'MB', 0.10),
    
    -- Indexing Performance
    (v_run_timestamp, 'PostgreSQL', 'indexing', 'query_improvement',
        (SELECT COALESCE(AVG(performance_improvement), 0) FROM benchmark_results.index_performance), 'x', 0.20),
    
    -- Security Overhead
    (v_run_timestamp, 'PostgreSQL', 'security', 'rls_overhead',
        (SELECT COALESCE(AVG(overhead_percentage), 0) FROM benchmark_results.security_overhead), '%', 0.05);
    
    -- Note: InfluxDB and MongoDB data would be imported from external sources
    -- For now, we'll insert placeholder data
    
    -- Insert InfluxDB placeholder metrics
    INSERT INTO benchmark_comparison.database_comparison 
    (run_timestamp, database_name, test_category, metric_name, metric_value, unit, weight, notes)
    VALUES 
    (v_run_timestamp, 'InfluxDB', 'ingestion', 'write_rate', 15000, 'points/sec', 0.25, 'Estimated from typical performance'),
    (v_run_timestamp, 'InfluxDB', 'query', 'time_series_query', 50, 'ms', 0.15, 'Estimated'),
    (v_run_timestamp, 'InfluxDB', 'storage', 'compressed_size', 350, 'MB', 0.10, 'Estimated compression'),
    (v_run_timestamp, 'InfluxDB', 'indexing', 'time_index_efficiency', 8.5, 'x', 0.20, 'Time-based queries'),
    (v_run_timestamp, 'InfluxDB', 'security', 'token_based_auth', 92, '%', 0.05, 'Security score');
    
    -- Insert MongoDB placeholder metrics
    INSERT INTO benchmark_comparison.database_comparison 
    (run_timestamp, database_name, test_category, metric_name, metric_value, unit, weight, notes)
    VALUES 
    (v_run_timestamp, 'MongoDB', 'ingestion', 'document_insert_rate', 8000, 'docs/sec', 0.25, 'Estimated'),
    (v_run_timestamp, 'MongoDB', 'query', 'document_query_latency', 75, 'ms', 0.15, 'Estimated'),
    (v_run_timestamp, 'MongoDB', 'storage', 'bson_size', 450, 'MB', 0.10, 'Estimated'),
    (v_run_timestamp, 'MongoDB', 'indexing', 'compound_index_gain', 12.3, 'x', 0.20, 'Compound indexes'),
    (v_run_timestamp, 'MongoDB', 'security', 'role_based_access', 88, '%', 0.05, 'Security score');
    
    -- Calculate and insert final scores
    PERFORM benchmark_comparison.calculate_final_scores(v_run_timestamp);
    
    COMMIT;
    
    RAISE NOTICE 'Comparison data populated for run: %', v_run_timestamp;
END;
$$;

-- Function to calculate final scores
CREATE OR REPLACE FUNCTION benchmark_comparison.calculate_final_scores(
    p_run_timestamp TIMESTAMP DEFAULT NULL
)
RETURNS VOID AS $$
BEGIN
    -- Clear existing scores for this run
    DELETE FROM benchmark_comparison.final_scores 
    WHERE (p_run_timestamp IS NULL OR run_timestamp = p_run_timestamp);
    
    -- Calculate and insert new scores
    INSERT INTO benchmark_comparison.final_scores 
    (run_timestamp, database_name, ingestion_score, query_score, storage_score, 
     indexing_score, security_score, total_score, ranking, performance_category)
    WITH normalized_scores AS (
        SELECT * FROM benchmark_comparison.calculate_normalized_scores(p_run_timestamp)
    ),
    category_scores AS (
        SELECT 
            database_name,
            test_category,
            AVG(normalized_score) as category_score
        FROM normalized_scores
        GROUP BY database_name, test_category
    ),
    pivot_scores AS (
        SELECT 
            database_name,
            MAX(CASE WHEN test_category = 'ingestion' THEN category_score END) as ingestion_score,
            MAX(CASE WHEN test_category = 'query' THEN category_score END) as query_score,
            MAX(CASE WHEN test_category = 'storage' THEN category_score END) as storage_score,
            MAX(CASE WHEN test_category = 'indexing' THEN category_score END) as indexing_score,
            MAX(CASE WHEN test_category = 'security' THEN category_score END) as security_score
        FROM category_scores
        GROUP BY database_name
    ),
    weighted_scores AS (
        SELECT 
            database_name,
            ingestion_score * 0.25 as w_ingestion,
            query_score * 0.25 as w_query,
            storage_score * 0.20 as w_storage,
            indexing_score * 0.20 as w_indexing,
            security_score * 0.10 as w_security,
            (ingestion_score * 0.25 + query_score * 0.25 + storage_score * 0.20 + 
             indexing_score * 0.20 + security_score * 0.10) as total_score
        FROM pivot_scores
    )
    SELECT 
        COALESCE(p_run_timestamp, CURRENT_TIMESTAMP),
        ws.database_name,
        ROUND(ws.w_ingestion, 2),
        ROUND(ws.w_query, 2),
        ROUND(ws.w_storage, 2),
        ROUND(ws.w_indexing, 2),
        ROUND(ws.w_security, 2),
        ROUND(ws.total_score, 2),
        RANK() OVER (ORDER BY ws.total_score DESC),
        CASE 
            WHEN ws.total_score >= 80 THEN 'Excellent'
            WHEN ws.total_score >= 60 THEN 'Good'
            WHEN ws.total_score >= 40 THEN 'Fair'
            ELSE 'Poor'
        END
    FROM weighted_scores ws;
END;
$$ LANGUAGE plpgsql;

-- View for comprehensive comparison report
CREATE OR REPLACE VIEW benchmark_comparison.comprehensive_report AS
SELECT 
    dc.database_name,
    dc.test_category,
    dc.metric_name,
    dc.metric_value,
    dc.unit,
    ns.normalized_score,
    ROUND(ns.normalized_score * dc.weight, 2) as weighted_score,
    dc.notes
FROM benchmark_comparison.database_comparison dc
LEFT JOIN benchmark_comparison.calculate_normalized_scores() ns
    ON dc.database_name = ns.database_name 
    AND dc.metric_name = ns.metric_name
ORDER BY dc.database_name, dc.test_category, dc.metric_name;

-- View for final ranking
CREATE OR REPLACE VIEW benchmark_comparison.final_ranking AS
SELECT 
    database_name,
    ingestion_score,
    query_score,
    storage_score,
    indexing_score,
    security_score,
    total_score,
    ranking,
    performance_category,
    CASE 
        WHEN ranking = 1 THEN '🏆 Winner'
        WHEN ranking = 2 THEN '🥈 Runner-up'
        WHEN ranking = 3 THEN '🥉 Third Place'
        ELSE 'Participant'
    END as award
FROM benchmark_comparison.final_scores
WHERE run_timestamp = (SELECT MAX(run_timestamp) FROM benchmark_comparison.final_scores)
ORDER BY ranking;

-- View for performance by category
CREATE OR REPLACE VIEW benchmark_comparison.category_performance AS
SELECT 
    test_category,
    database_name,
    ROUND(AVG(normalized_score), 2) as average_score,
    RANK() OVER (PARTITION BY test_category ORDER BY AVG(normalized_score) DESC) as category_rank
FROM benchmark_comparison.calculate_normalized_scores()
GROUP BY test_category, database_name
ORDER BY test_category, category_rank;

-- Procedure to generate comparison report
CREATE OR REPLACE PROCEDURE benchmark_comparison.generate_comparison_report()
LANGUAGE plpgsql
AS $$
DECLARE
    v_report_text TEXT;
    v_winner RECORD;
BEGIN
    -- Get the winner
    SELECT * INTO v_winner FROM benchmark_comparison.final_ranking WHERE ranking = 1;
    
    -- Build report
    v_report_text := 'DATABASE BENCHMARK COMPARISON REPORT' || E'\n';
    v_report_text := v_report_text || '=====================================' || E'\n' || E'\n';
    v_report_text := v_report_text || 'Generated: ' || CURRENT_TIMESTAMP || E'\n' || E'\n';
    
    v_report_text := v_report_text || '🏆 OVERALL WINNER: ' || v_winner.database_name || E'\n';
    v_report_text := v_report_text || '   Total Score: ' || v_winner.total_score || '/100' || E'\n';
    v_report_text := v_report_text || '   Category: ' || v_winner.performance_category || E'\n' || E'\n';
    
    v_report_text := v_report_text || 'FINAL RANKINGS:' || E'\n';
    v_report_text := v_report_text || '---------------' || E'\n';
    
    FOR r IN SELECT * FROM benchmark_comparison.final_ranking ORDER BY ranking LOOP
        v_report_text := v_report_text || r.ranking || '. ' || r.database_name || ' - ' || r.total_score || '/100';
        IF r.ranking <= 3 THEN
            v_report_text := v_report_text || ' ' || r.award;
        END IF;
        v_report_text := v_report_text || E'\n';
    END LOOP;
    
    v_report_text := v_report_text || E'\n' || 'CATEGORY WINNERS:' || E'\n';
    v_report_text := v_report_text || '-----------------' || E'\n';
    
    FOR c IN 
        SELECT DISTINCT ON (test_category) 
            test_category, database_name, average_score
        FROM benchmark_comparison.category_performance
        ORDER BY test_category, category_rank
    LOOP
        v_report_text := v_report_text || 
            '• ' || c.test_category || ': ' || c.database_name || 
            ' (' || c.average_score || ')' || E'\n';
    END LOOP;
    
    -- Output report
    RAISE NOTICE '%', v_report_text;
    
    -- Also save to a table
    INSERT INTO benchmark_comparison.report_history (report_text, generated_at)
    VALUES (v_report_text, CURRENT_TIMESTAMP);
    
END;
$$;

-- Table for report history
CREATE TABLE IF NOT EXISTS benchmark_comparison.report_history (
    report_id SERIAL PRIMARY KEY,
    report_text TEXT,
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Materialized view for fast analytics
CREATE MATERIALIZED VIEW IF NOT EXISTS benchmark_comparison.performance_trends AS
SELECT 
    DATE(run_timestamp) as run_date,
    database_name,
    AVG(total_score) as avg_daily_score,
    COUNT(*) as runs_count
FROM benchmark_comparison.final_scores
GROUP BY DATE(run_timestamp), database_name
ORDER BY run_date DESC, avg_daily_score DESC;

-- Refresh function for materialized view
CREATE OR REPLACE FUNCTION benchmark_comparison.refresh_performance_trends()
RETURNS TRIGGER AS $$
BEGIN
    REFRESH MATERIALIZED VIEW benchmark_comparison.performance_trends;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Trigger to auto-refresh materialized view
CREATE TRIGGER refresh_trends_trigger
AFTER INSERT ON benchmark_comparison.final_scores
FOR EACH STATEMENT
EXECUTE FUNCTION benchmark_comparison.refresh_performance_trends();

-- Export results to CSV
CREATE OR REPLACE PROCEDURE benchmark_comparison.export_to_csv()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Export final ranking
    COPY (
        SELECT * FROM benchmark_comparison.final_ranking
    ) TO '/tmp/benchmark_final_ranking.csv' WITH CSV HEADER;
    
    -- Export detailed comparison
    COPY (
        SELECT * FROM benchmark_comparison.comprehensive_report
    ) TO '/tmp/benchmark_detailed_comparison.csv' WITH CSV HEADER;
    
    -- Export category performance
    COPY (
        SELECT * FROM benchmark_comparison.category_performance
    ) TO '/tmp/benchmark_category_performance.csv' WITH CSV HEADER;
    
    RAISE NOTICE 'Results exported to CSV files in /tmp/ directory';
END;
$$;

-- Initialize with sample data (for testing)
CALL benchmark_comparison.populate_comparison_data();

-- Generate initial report
CALL benchmark_comparison.generate_comparison_report();

-- Export initial results
CALL benchmark_comparison.export_to_csv();