with gr_rev as (select date(date_trunc('month', payment_date)) period_month,
                       user_id,
                       sum(revenue_amount_usd) month_revenue
                from project.games_payments
                group by date(date_trunc('month', payment_date)),user_id
               ),
revenue as (select  period_month, 
                  user_id,
                  sum(month_revenue) month_revenue,
                  lag(sum(month_revenue)) over( partition by user_id order by period_month) prev_pay_revenue,
                  lag(period_month) over( partition by user_id order by period_month) prev_pay_month,
                  lead(period_month) over( partition by user_id order by period_month) next_pay_month,
                  date(period_month + interval'1 month') next_calendar,
                  date(period_month - interval'1 month') prev_calendar,
                  count(period_month) over (partition by user_id)/
                  (extract(month from age(max(period_month) over(partition by user_id), 
                                         min(period_month) over(partition by user_id)))+1) coeff_mrr
                  from gr_rev
                  group by user_id,
                  period_month
                  ),
all_rec as (select period_month,
                    user_id,
                    month_revenue,
                    'revenue' type_r,
                    coeff_mrr
             from revenue
             union all
             select next_calendar,
                    user_id,
                    month_revenue,
                    'new_mrr' type_r,
                    coeff_mrr
             from revenue
             where prev_pay_month is null 
             union all 
             select next_calendar,
                    user_id,
                    -month_revenue,
                    'churned_revenue' type_r,
                    coeff_mrr
             from revenue
             where next_pay_month is null or next_calendar!=next_pay_month
             union all
             select period_month,
                    user_id,
                    month_revenue,
                    'expantion' type_r,
                    coeff_mrr
             from revenue
             where month_revenue>prev_pay_revenue and prev_pay_month=prev_calendar
             union all 
             select period_month,
                    user_id,
                    -month_revenue,
                    'contraction' type_r,
                    coeff_mrr
             from revenue
              where month_revenue<prev_pay_revenue and prev_pay_month=prev_calendar
              union all 
              select next_calendar,
                    user_id,
                    month_revenue,
                    'back_churned_revenue' type_r,
                    coeff_mrr
             from revenue
             where prev_pay_month is not null and prev_calendar!=prev_pay_month
             )
select t.*,game_name,language,has_older_device_model
from all_rec  t
inner join project.games_paid_users gpu on gpu.user_id=t.user_id