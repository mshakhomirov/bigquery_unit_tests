-- ======================================================== --
--       TESTING your dataset  OR  BigQuery table           --
-- ======================================================== --

-- Let's imagine we have some base table which we need to test. For this example I will use a sample with user subscription transactions. This is a very common case for many online applications where
-- users can  make in-app purchases, for example, subscriptions and they may or may not expire in the future.
-- Run this SQL below for testData1 to see this table example. It's a CTE and it contains information, e.g. user_id, product_id, transaction_id, created_at (a timestamp when this transaction was created) and
-- expire_time_after_purchase which is a timestamp expiration for that subscription.

-- test 1: I want to be sure that this base table doesn't have duplicates.
-- For example, 'For every (transaction_id) there is one and only one (created_at): '
-- test table d always imitates areal-life scenario from purchase summary table:
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
...
'Do something here.'
--Here's our first test:

--a select statement that catenates the test name and the case statement
select concat( 
-- the test name
'For every (transaction_id) there is one and only one (created_at): ', 
-- the case statement
   case when 
-- one or more subqueries
-- in this case, an expected value and an actual value 
-- that must be equal for the test to pass
  ( select count(distinct transaction_id) from testData1) 
  --expected value,
  = ( select count(distinct created_at) from testData1)  
  -- actual value
  -- the then and else branches of the case statement
  then 'passed' else 'failed' end
  -- close the concat function and terminate the query 
  ); 
  -- test result.

  --Test2.
--   Now let's test it's consecutive, e.g. our base table is sorted in the way we need it. Even though BigQuery works with sets and doesn't use internal sorting  
--   we can ensure that our table is sorted, e.g. consequtive numbers of transactions are in order with created_at timestmaps:
  
select concat( 'transaction_ids are consecutive: ',
    case when (
        select count(*)
        from (
            select * 
            , ROW_NUMBER()  OVER(order by created_at)       AS created_at_rank
            , ROW_NUMBER()  OVER(order by transaction_id)   AS transaction_id_rank
            from testData1 a 
        ) a
        where a.created_at_rank <> a.transaction_id_rank 
        
    ) = 0 
    then 'passed' else 'failed' end );

    -- Now let's wrap these two tests together:

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

--test1:
select concat( 'For every (transaction_id) there is one and only one (created_at): ', 
    case when ( select count(distinct transaction_id) from testData1) -- expected value
        = ( select count(distinct created_at) from testData1)         -- actual value
    then 'passed' else 'failed' end
) AS Test_results
UNION ALL   
-- test2:
select concat( 'Transaction_ids are consecutive: ',
    case when (
        select count(*)
        from (
            select * 
            , ROW_NUMBER()  OVER(order by created_at)       AS created_at_rank
            , ROW_NUMBER()  OVER(order by transaction_id)   AS transaction_id_rank
            from testData1 a 
        ) a
        where a.created_at_rank <> a.transaction_id_rank 
        
    ) = 0 
    then 'passed' else 'failed' end )
  ; 


    -- Even though you now can run multiple queries subsequently it would be better and easier to create a unit test template.
    -- Healthcheck example:
    select count(*) FROM `your-project.production.purchase_summary` ps;
    select count(*) FROM `your-project.production.payment_transaction` p;

    -- Imagine you're testing a new query which is going to become a new table (or a dataset) soon and you need to perform SQL tests ON THAT query. 
    -- It becomes even more handy to use a template or stored procedure when you need to get one single set of test results for your new dataset query. 
    -- Imagine you are going to create a CI/CD pipeline which MUST include such test and won't let you PUSH to master branch in your repo.
    -- That's where stored procedures become useful.

    -- Let's imagine that our testData1 dataset which we created and tested above will be passed into a function. we might want to do that if we need
    -- to iteratively process each row and the desired outcome can't be achieved with standard SQL.
    -- Scenario.
    -- In this example we are going to "stack up" expire_time_after_purchase based on previous value and the fact that the previous purchase expired or not.
    -- Then we need to test the UDF properly.
    -- We will also create a nifty script that does this trick.

CREATE TEMPORARY FUNCTION 
    process(table ARRAY<STRUCT<user_id INT64, product INT64, expire_time_after_purchase TIMESTAMP, transaction_id INT64, created_at TIMESTAMP>>)
RETURNS           ARRAY<STRUCT<user_id INT64, product INT64, expire_time_after_purchase TIMESTAMP, transaction_id INT64, created_at TIMESTAMP>>
LANGUAGE js AS """
    
    function AddDaysToDate(date, days) {
        let date_obj = new Date(date);
    return new Date(date_obj.getTime() + (days*24*60*60*1000) );
    };

    let i;  
    for(i = 0; i < table.length; i++) {
        // IF expire_time_after_purchase IS NULL, iterativly calculate a new one for each row in this grouping based on previous value:
        if(i==0) { 
            table[i].expire_time_after_purchase = AddDaysToDate(table[i].created_at, 30)  
        }

        //For all the other purchase which are new, calculate new expire_time_after_purchase based on previous value:
        // check if previous purchase has expired first:
        else {
            //if expired
            if (table[i].created_at > table[i-1].expire_time_after_purchase) {
                table[i].expire_time_after_purchase = AddDaysToDate(table[i].created_at, 30)
            }
            //if not 
            else {
                table[i].expire_time_after_purchase = AddDaysToDate(table[i-1].expire_time_after_purchase, 30)
            }
            
        }

    }
    return table;
""";

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

SELECT   
             ARRAY_AGG(STRUCT(user_id, product_id AS product,expire_time_after_purchase,transaction_id, created_at) )  AS original
    ,process(ARRAY_AGG(STRUCT(user_id, product_id AS product,expire_time_after_purchase,transaction_id, created_at) )) AS processed
FROM
    testData1 
    ;

    --  What we need to test now is how does this function calculates new expire_time_after_purchase time. You can see it under `processed` column.
    -- I will now create a series of tests for this and then I will use a BigQuery script to iterate through each testing use case to see if my function fails.
    -- But first we will need an `expected` value for each test.
    -- let's slightly change our testData1 and add `expected` column:

    -- UDF goes here first:
    --...
    CREATE TEMPORARY FUNCTION 
    process(table ARRAY<STRUCT<user_id INT64, product INT64, expire_time_after_purchase TIMESTAMP, transaction_id INT64, created_at TIMESTAMP, expected TIMESTAMP>>)
RETURNS           ARRAY<STRUCT<user_id INT64, product INT64, expire_time_after_purchase TIMESTAMP, transaction_id INT64, created_at TIMESTAMP, expected TIMESTAMP>>
LANGUAGE js AS """
    
    function AddDaysToDate(date, days) {
        let date_obj = new Date(date);
    return new Date(date_obj.getTime() + (days*24*60*60*1000) );
    };

    let i;  
    for(i = 0; i < table.length; i++) {
        // IF expire_time_after_purchase IS NULL, iterativly calculate a new one for each row in this grouping based on previous value:
        if(i==0) { 
            table[i].expire_time_after_purchase = AddDaysToDate(table[i].created_at, 30)  
        }

        //For all the other purchase which are new, calculate new expire_time_after_purchase based on previous value:
        // check if previous purchase has expired first:
        else {
            //if expired
            if (table[i].created_at > table[i-1].expire_time_after_purchase) {
                table[i].expire_time_after_purchase = AddDaysToDate(table[i].created_at, 30)
            }
            //if not 
            else {
                table[i].expire_time_after_purchase = AddDaysToDate(table[i-1].expire_time_after_purchase, 30)
            }
            
        }

    }
    return table;
""";

    with testData1 as (
    select * from unnest([
    struct
    (1799867122 as user_id, 158 as product_id, timestamp (null) as expire_time_after_purchase,  70000000 as transaction_id, timestamp '2020-11-23 09:01:00' as created_at, timestamp '2020-12-23 09:01:00 UTC' as expected ),
    (1799867122,158,timestamp (null),70000001,timestamp '2020-11-23 09:15:00.042308 UTC',timestamp '2021-01-22 09:01:00 UTC'),
    (1799867122,158,timestamp (null),70000002,timestamp '2020-11-23 09:30:00.042308 UTC',timestamp '2021-02-21 09:01:00 UTC'),
    (1799867122,158,timestamp (null),70000003,timestamp '2020-11-23 09:45:00.042308 UTC',timestamp '2021-03-23 09:01:00 UTC')
    ]
    ) as t
  )

select 
     test.transaction_id
    ,test.created_at
    ,test.expire_time_after_purchase
    ,test.expected
    ,IF(test.expire_time_after_purchase!=test.expected, true, false) as isFailed
from (
select 
    process(ARRAY_AGG(STRUCT(user_id, product_id AS product,expire_time_after_purchase,transaction_id, created_at, expected) )) AS processed
    FROM testData1 ) t
    ,unnest(processed) test
    ;

    -- So now if we run it we can see test results for each row straight away:
    >> image

    -- If we change our JS UDF to this:
    ...
    //if not 
            else {
                table[i].expire_time_after_purchase = AddDaysToDate(table[i-1].expire_time_after_purchase, 31)
            }
    ...
    -- and then run a test again we will see where it fails:

    >> image

    -- Now let's imagine that we need a clear test for a particular case when the data has changed. By `clear` I mean the situation whish is easier to understand.
    -- Of course, we could incorporate that second scenario in our 1st test for UDF but separating and simplifying makes a code esier to understand, replicate and
    -- use later.

    -- - test2: Let's say we have a purchase that expired inbetween. In the exmaple below purchase with transaction 70000001 expired at 2021-01-22 09:01:00 and stucking MUST stop here
    -- until the next purchase.
    with testData2 as (
        select * from unnest([
        struct
        (1799867122 as user_id, 158 as product_id, timestamp (null) as expire_time_after_purchase,  70000000 as transaction_id, timestamp '2020-11-23 09:01:00' as created_at, timestamp '2020-12-23 09:01:00 UTC' as expected ),
        (1799867122,158,timestamp (null),70000001,timestamp '2020-11-23 09:15:00.042308 UTC',timestamp '2021-01-22 09:01:00 UTC'),
        (1799867122,158,timestamp (null),70000002,timestamp '2021-01-23 09:30:00.042308 UTC',timestamp '2021-02-22 09:30:00.042 UTC'),
        (1799867122,158,timestamp (null),70000003,timestamp '2021-01-23 09:45:00.042308 UTC',timestamp '2021-03-25 09:30:00.042 UTC')
        ]
        ) as t
    )

SELECT   
    process(ARRAY_AGG(STRUCT(user_id, product_id AS product,expire_time_after_purchase,transaction_id, created_at, expected) )) AS processed
FROM
    testData2 
;
    -- and of course we need a test 3 to make sure that data inside each array of structs is sorted before it is passsed into UDF. UDF doesn't care about sorting. It will simply process
    -- rows in that exact order they we passed.

    -- Now we could use UNION ALL to run a SELECT query for each test case and by doingo so generate the test output. However that might significantly increase the test.sql file size and
    -- make it much more difficult to read.
    -- Instead it would be much easier to user BigQuery scripting to iterate through each test case's data, generate test results for each case and insert all results into one table in order to
    -- produce one single output.
    -- BigQuery scripting enables you to send multiple statements to BigQuery in one request, to use variables, and to use control flow statements such as IF and WHILE.

    -- Let'c create our function first and store it in our data warehouse:
    -- To create a persistent UDF, use the following SQL:
    CREATE OR REPLACE FUNCTION `your-project.staging.process_purchase`(table ARRAY<STRUCT<user_id INT64, product INT64, expire_time_after_purchase TIMESTAMP, transaction_id INT64, created_at TIMESTAMP , expected TIMESTAMP>>)
RETURNS           ARRAY<STRUCT<user_id INT64, product INT64, expire_time_after_purchase TIMESTAMP, transaction_id INT64, created_at TIMESTAMP, expected TIMESTAMP>>
LANGUAGE js AS """
    
    function AddDaysToDate(date, days) {
        let date_obj = new Date(date);
    return new Date(date_obj.getTime() + (days*24*60*60*1000) );
    };

    let i;  
    for(i = 0; i < table.length; i++) {
        // IF expire_time_after_purchase IS NULL, iterativly calculate a new one for each row in this grouping based on previous value:
        if(i==0) { 
            table[i].expire_time_after_purchase = AddDaysToDate(table[i].created_at, 30)  
        }

        //For all the other purchase which are new, calculate new expire_time_after_purchase based on previous value:
        // check if previous purchase has expired first:
        else {
            //if expired
            if (table[i].created_at > table[i-1].expire_time_after_purchase) {
                table[i].expire_time_after_purchase = AddDaysToDate(table[i].created_at, 30)
            }
            //if not 
            else {
                table[i].expire_time_after_purchase = AddDaysToDate(table[i-1].expire_time_after_purchase, 30)
            }
            
        }

    }
    return table;
""";

CREATE or replace PROCEDURE `your-project.staging.RunPurchaseSummarySQLTest`()
BEGIN
        
    
    
    -- Let's create a procedure to iterate through array of our UDF tests:
    DECLARE tests ARRAY<STRING>;
    DECLARE test_query STRING DEFAULT '';
    DECLARE udf string;
    DECLARE i INT64 DEFAULT 0;

    -- UDF tests ti iterate through:
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
         '%s' as test_id 
        ,test.transaction_id
        ,test.created_at
        ,test.expire_time_after_purchase
        ,test.expected
        ,IF(test.expire_time_after_purchase!=test.expected, true, false) as isFailed
    from (
    select 
        `your-project`.staging.process_purchase(ARRAY_AGG(STRUCT(user_id, product_id AS product,expire_time_after_purchase,transaction_id, created_at, expected) )) AS processed
    FROM %s 
    ) t
    ,unnest(processed) test
     
    """;


    -- Create a temporary table called test_results.
    EXECUTE IMMEDIATE
    "CREATE TEMP TABLE test_results (test_id STRING,transaction_id INT64,	created_at TIMESTAMP,	expire_time_after_purchase TIMESTAMP,	expected TIMESTAMP,	isFailed BOOLEAN)";

    LOOP
        SET i = i + 1;

        IF i > ARRAY_LENGTH(tests) THEN 
            LEAVE;
        END IF;

        IF i > 0 THEN
        
            EXECUTE IMMEDIATE "INSERT test_results" || format(test_query,tests[ORDINAL(i)],  tests[ORDINAL(i)] );
        END IF;

    END LOOP;
     
    EXECUTE IMMEDIATE "SELECT * FROM test_results;";

END;





BEGIN
  CALL staging.RunPurchaseSummarySQLTest();
EXCEPTION WHEN ERROR THEN
  SELECT
    @@error.message,
    @@error.stack_trace,
    @@error.statement_text,
    @@error.formatted_stack_trace;
END;


-- Done! Now we have a procedure to iteratively run the tests we need:
>> image.

-- Now in ideal scenario we probably would like to chain our isolated unit tests all together and perform them all in one procedure.
-- Let's chain first two checks from the very beginning with our UDF checks:

file .chain_bq_unit_tests.sql



-- Now let's do one more thing (optional) - convert our test results to a JSON string. Who knows, maybe you'd like to run your test script programmatically and get a result as a response in ONE JSON row. 
-- Let's simply change the ending of our stored procedure to this:

EXECUTE IMMEDIATE "SELECT CONCAT('[', STRING_AGG(TO_JSON_STRING(t), ','), ']') data FROM test_results t;";

    
-- Performing simple healthcheks:
-- We can extend this example by simulating an outage in an upstream data table (as if all records were accidentally removed). One of the ways you can guard against reporting on a faulty upstream data source is by adding health checks using the BigQuery ERROR() function:
-- https://wideops.com/whats-happening-in-bigquery-new-features-bring-flexibility-and-scale-to-your-data-warehouse/

-- Now let's imagine our pipeline is up and running processing new records. What I would like to do is to monitor every time it does the transformation and data load.
-- One of the ways you can guard against reporting on a faulty data streams is by adding health checks using the BigQuery ERROR() function.
-- We can now schedule this query to run hourly for example and receive notification if error was raised:

    SELECT COUNT(*) as row_count FROM yourDataset.yourTable 
    HAVING IF(row_count > ), true, ERROR(FORMAT('ERROR: row count must be > 0 but is %t',row_count)));
-- This will raise an error and the query will be run only if the previous healthcheck passed.
-- However, it might be worth trying to design SQL flow in a way where each block will not rely on the previous one. This would guarantee a smooth data pipeline execution without errors in 
-- resulting datasets.

