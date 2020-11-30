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
    ,null as info 


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