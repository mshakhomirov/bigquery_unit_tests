-- prereq: You must have already created a UDF.


CREATE or replace PROCEDURE `your-project.staging.RunBQSQLUnitTests`()
BEGIN
        
    
    
    -- Let's create a procedure to iterate through array of our UDF tests:
    DECLARE tests ARRAY<STRING>;
    DECLARE test_query STRING DEFAULT '';
    DECLARE data_test_query STRING DEFAULT '';
    DECLARE udf string;
    DECLARE i INT64 DEFAULT 0;

    -- UDF tests:
    set tests = ['test1', 'test2'];

    set udf = 
    """
    UDF won't work if you declare it here as TEMP. Read more: https://cloud.google.com/bigquery/docs/reference/standard-sql/scripting
    BigQuery interprets any request with multiple statements as a script, unless the statements consist of CREATE TEMP FUNCTION statement(s), with a single final query statement.

    Also I was unable to call a UDF from my script until I actually created a persistent stored procedure.  
    """;

    set test_query = """
    with test1 as (
    select * from unnest([
    struct
    (1799867122 as user_id, 158 as product_id, timestamp (null) as expire_time_after_purchase,  70000000 as transaction_id, timestamp '2020-11-23 09:01:00' as created_at, timestamp '2020-12-23 09:01:00 UTC' as expected ),
    (1799867122,158,timestamp (null),70000001,timestamp '2020-11-23 09:15:00.042308 UTC',timestamp '2021-01-22 09:01:00 UTC'),
    (1799867122,158,timestamp (null),70000002,timestamp '2020-11-23 09:30:00.042308 UTC',timestamp '2021-02-21 09:01:00 UTC'),
    (1799867122,158,timestamp (null),70000003,timestamp '2020-11-23 09:45:00.042308 UTC',timestamp '2021-03-23 09:01:00 UTC')
    ]
    ) as t
    )
    ,
    test2 as (
        select * from unnest([
        struct
        (1799867122 as user_id, 158 as product_id, timestamp (null) as expire_time_after_purchase,  70000000 as transaction_id, timestamp '2020-11-23 09:01:00' as created_at, timestamp '2020-12-23 09:01:00 UTC' as expected ),
        (1799867122,158,timestamp (null),70000001,timestamp '2020-11-23 09:15:00.042308 UTC',timestamp '2021-01-22 09:01:00 UTC'),
        (1799867122,158,timestamp (null),70000002,timestamp '2021-01-23 09:30:00.042308 UTC',timestamp '2021-02-22 09:30:00.042 UTC'),
        (1799867122,158,timestamp (null),70000003,timestamp '2021-01-23 09:45:00.042308 UTC',timestamp '2021-03-25 09:30:00.042 UTC')
        ]
        ) as t
    )
    select
         'UDF %s' as test_id
        ,max(IF(test.expire_time_after_purchase!=test.expected, true, false)) as isFailed -- do IN UNNEST([]) to check instead
        ,ARRAY_AGG(STRUCT(
            test.transaction_id
            ,test.created_at
            ,test.expire_time_after_purchase
            ,test.expected
            ,IF(test.expire_time_after_purchase!=test.expected, true, false) as isFailed
        )) as udf_test_results
    from (
    select 
        `your-project`.staging.process_purchase(ARRAY_AGG(STRUCT(user_id, product_id AS product,expire_time_after_purchase,transaction_id, created_at, expected) )) AS processed
    FROM %s 
    ) t
    ,unnest(processed) test
    
    group by 1
     
    """;




    

    -- Create a temporary table called test_results.
    EXECUTE IMMEDIATE
     "CREATE TEMP TABLE test_results (test_id STRING,isFailed BOOLEAN,info ARRAY<STRUCT<transaction_id INT64, created_at TIMESTAMP, expire_time_after_purchase TIMESTAMP, expected TIMESTAMP, isFailed BOOLEAN>>)";

    LOOP
        SET i = i + 1;

        IF i > ARRAY_LENGTH(tests) THEN 
            LEAVE;
        END IF;

        IF i > 0 THEN
        
            EXECUTE IMMEDIATE "INSERT test_results" || format(test_query,tests[ORDINAL(i)],  tests[ORDINAL(i)] );
        END IF;

    END LOOP;

    -- Now add dataset tests from the beginning:
    SET  data_test_query =
    """
    insert test_results(test_id, isFailed)

    with testData1 as (
    select * from unnest([
    struct
    (1799867122 as user_id, 158 as product_id, timestamp (null) as expire_time_after_purchase,  70000000 as transaction_id, timestamp '2020-11-23 09:01:00' as created_at),
    (1799867122,158,timestamp (null),70000001,timestamp '2020-11-23 09:15:00.042308 UTC'),
    (1799867122,158,timestamp (null),70000002,timestamp '2020-11-23 09:30:00.042308 UTC'),
    (1799867122,158,timestamp (null),70000003,timestamp '2020-11-23 09:45:00.042308 UTC')
    ]
    ) as t
    )

    select test_id 
        , isFailed as isFailed 

    from (    
        -- test1:
        select  
            'For every (transaction_id) there is one and only one (created_at): ' as test_id 
            , case when ( select count(distinct transaction_id) from testData1) -- expected value
                = ( select count(distinct created_at) from testData1)         -- actual value
            then false else true end
            AS isFailed
        
        UNION ALL   

        -- test2:
        select  
            'Transaction_ids are consecutive: ' as test_id
            ,case when (
                select count(*)
                from (
                    select * 
                    , ROW_NUMBER()  OVER(order by created_at)       AS created_at_rank
                    , ROW_NUMBER()  OVER(order by transaction_id)   AS transaction_id_rank
                    from testData1 a 
                ) a
                where a.created_at_rank <> a.transaction_id_rank 
                
            ) = 0 
            then false else true end 
        ) r
    ;
    """
    ;
    EXECUTE IMMEDIATE data_test_query;

    EXECUTE IMMEDIATE "SELECT * FROM test_results;";

END;

BEGIN
  CALL staging.RunBQSQLUnitTests();
EXCEPTION WHEN ERROR THEN
  SELECT
    @@error.message,
    @@error.stack_trace,
    @@error.statement_text,
    @@error.formatted_stack_trace;
END;

-- to run from the shell:
-- bq query --use_legacy_sql=false 'CALL staging.RunPurchaseSummarySQLTest();'
