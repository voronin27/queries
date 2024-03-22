with tmp as (select event_timestamp,
                    event_params,
                    traffic_source,
                    event_name,
                    user_pseudo_id
              FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
              where  _TABLE_SUFFIX BETWEEN '20210101' AND '2021231' 
             ),
it as (
        SELECT  cast (TIMESTAMP_MICROS(event_timestamp) as date) as event_date,
                traffic_source.source,
                traffic_source.medium,
                max(if(event_name='add_to_cart',1,0)) add_to_cart,
                max(if(event_name='begin_checkout',1,0)) begin_checkout, 
                max(if(event_name='purchase',1,0)) purchase,
                (SELECT value.string_value
                  FROM
                 UNNEST(event_params)
                WHERE key = 'campaign') AS event_campaign,
                 (SELECT COALESCE(value.int_value, value.float_value, value.double_value)  
                FROM UNNEST(event_params)
                WHERE key = 'ga_session_id'
                ) AS session_id,
                user_pseudo_id,
                coalesce(max((select COALESCE(value.int_value, value.float_value, value.double_value) from unnest(event_params) where key='session_engaged')),0) session_engaged,
                coalesce(sum((select value.int_value from unnest(event_params) where key='engagement_time_msec')),0)  engagement_time_msec
               


        FROM tmp 

        group by event_date,                                 
                 traffic_source.source,                         
                 traffic_source.medium,
                 session_id,
                 event_campaign,
                 user_pseudo_id

        ),
 page_path as (
                select user_pseudo_id,
                        (SELECT COALESCE(value.int_value, value.float_value, value.double_value)  
                        FROM UNNEST(event_params)
                        WHERE key = 'ga_session_id'
                        ) AS session_id,
                        SAFE_CAST(REGEXP_EXTRACT((SELECT value.string_value
                                                  FROM
                                                  UNNEST(event_params)
                                                WHERE key = 'page_location'),
                                                r'^https?://[^/]+/([^?]+)(?:\?|$)') AS STRING
                                  ) AS page -- виборка з URL page path
                         

                from  tmp
                where event_name in('session_start') 
                )
select --event_date,
      --  source,
      --  medium,
      --  event_campaign,
        page,
        count(distinct i.session_id||i.user_pseudo_id) count_of_session, 
        sum(purchase) purchase_count,
       -- round(sum(purchase)/count(distinct i.session_id||i.user_pseudo_id)*100,2) purchase_convers,
        corr(session_engaged,engagement_time_msec) corr_engage,  
        corr(purchase,engagement_time_msec) corr_purchase  
                                                           
from it i
inner join page_path pp on pp.user_pseudo_id=i.user_pseudo_id and pp.session_id=i.session_id
group by 
        page
