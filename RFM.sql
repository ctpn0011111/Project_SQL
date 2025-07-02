-- practice RFM Analysis

--B1 Tính RFM

with table_rfm as (
    select  customer_id,
        recency = DATEDIFF(day, max(transaction_date), '2018-12-31'), -- KC ngày mua hàng gần nhất tới 31-12-2018
        frequency = count(order_id), -- đếm số đơn hàng
        monetary = sum(cast(final_price as bigint))
    from (
        select * from payment_history_17 union select * from payment_history_18
        
    ) as table_his
    join product as pro
    on table_his.product_id = pro.product_number
    where message_id = 1
    -- and sub_category = 'Electricity'
    group by customer_id)

-- B2: Tính vị trí theo percentile
,table_rank as (
    select *,
        PERCENT_RANK() over(order by recency asc) as r_rank,
        PERCENT_RANK() over(order by frequency desc) as f_rank,
        PERCENT_RANK() over(order by monetary desc) as m_rank
    from table_rfm
)

-- B3: Nhóm thành 4 tier cho mỗi chỉ số
, table_tier as (
    select *,
        (case when r_rank <= 0.25 then 1
            when r_rank <= 0.5 then 2
            when r_rank <= 0.75 then 3
            else 4
        end) as r_tier,

        (case when f_rank <= 0.25 then 1
            when f_rank <= 0.5 then 2
            when f_rank <= 0.75 then 3
            else 4
        end) as f_tier,

        (case when m_rank <= 0.25 then 1
            when m_rank <= 0.5 then 2
            when m_rank <= 0.75 then 3
            else 4
        end) as m_tier
    from table_rank
)
,table_score as (
    select *,
        concat(r_tier, f_tier, m_tier) as rfm_score
    from table_tier
)

-- B4: Phân nhóm theo tổ hợp hành vi
, table_segment as (
    select *,
        case when rfm_score = 111 then 'Best Customers'
            when rfm_score like '[3-4][3-4][1-4]' then 'Lost Bad Customers'
            when rfm_score like '[3-4]2[1-4]' then 'Lost Customers'
            when rfm_score like '21[1-4]' then 'Almost Lost'
            when rfm_score like '11[2-4]' then 'Loyal Customers'
            when rfm_score like '[1-2][1-3]1' then 'Big Spenders'
            when rfm_score like '[1-2]4[1-4]' then 'New Customers'
            when rfm_score like '[3-4]1[1-4]' then 'Hibernating'
            when rfm_score like '[1-2][2-3][2-4]' then 'Potential Loyalists'
        else 'unknown' end as segment
    from table_score
 
)


SELECT segment,
    count(customer_id) as [Count_customers],
    (select count(customer_id) from table_segment) as [total_customers],
    format(cast(count(customer_id) as float) / (select count(customer_id) from table_segment), 'p') as [percent]
from table_segment
group by segment
order by [Count_customers] desc;

-- PHÂN TÍCH CHI TIẾT TỪNG NHÓM KHÁCH HÀNG
-- Tính % đơn hàng pro/normal trên tổng đơn hàng của từng khách
with table_joined as (
    select customer_id, order_id, promotion_id, transaction_date
    from (
        select * from payment_history_17
        union all select * from payment_history_18
    ) as table_union
    join product as pro
    on table_union.product_id = pro.product_number
    where customer_id in (
        select customer_id from table_tonghop
        where segment = 'Best Customers'
    )
    and message_id = 1
)
, table_total_order as (
    select customer_id, 
        count(case when promotion_id <> '0' then order_id end) as total_order_pro,
        count(case when promotion_id = '0' then order_id end) as total_order_normal 
    from table_joined
    group by customer_id
)
select *,
    format(cast(total_order_pro as float) / (total_order_pro + total_order_normal), 'p') as [percent_pro],
    format(cast(total_order_normal as float) / (total_order_pro + total_order_normal), 'p') as [percent_pro]
from
table_total_order


-- Tính tỉ trọng đơn hàng theo pro và normal
with table_joined as (
    select customer_id, order_id, promotion_id, transaction_date
    from (
        select * from payment_history_17
        union all select * from payment_history_18
    ) as table_union
    join product as pro
    on table_union.product_id = pro.product_number
    where customer_id in (
        select customer_id from table_tonghop
        where segment = 'Best Customers'
    )
    and message_id = 1
)
, table_total_order as (
    select 
        count(case when promotion_id <> '0' then order_id end) as total_order_pro,
        count(case when promotion_id = '0' then order_id end) as total_order_normal 
    from table_joined
)
select total_order_pro,
    total_order_normal,
    format(cast(total_order_pro as float) /  (total_order_pro + total_order_normal ), 'p') as [percent_pro],
    format(cast(total_order_normal as float) /  (total_order_pro + total_order_normal ), 'p') as [percent_normal]
from table_total_order


-- Khách hàng "Best Customers" thường mua hàng vào ngày nào
with table_joined as (
    select customer_id, order_id, transaction_date
    from (
        select * from payment_history_17
        union all select * from payment_history_18
    ) as table_union
    join product as pro
    on table_union.product_id = pro.product_number
    where customer_id in (
        select customer_id from table_tonghop
        where segment = 'Best Customers'
    )
    and message_id = 1
)
, table_weekday as (
    select customer_id,
        order_id,
        month(transaction_date) as [month],
        datename(weekday, transaction_date) as [weekday]
    from table_joined
)
, table_rank as (
    select customer_id, [weekday],
        count(order_id) as [count_order],
        row_number() over(partition by customer_id order by count(order_id) desc) as [rank]
    from table_weekday
    group by customer_id, [weekday]
)
select [weekday], 
    count(case when rank = 1 then [weekday] end) as [count_order_by_day]
from table_rank
where rank < 5
group by [weekday]
order by count(case when rank = 1 then [weekday] end) desc


-- Top nhóm sản phẩm được mua nhiều nhất
with table_joined as (
    select customer_id, order_id, category
    from (
        select * from payment_history_17
        union all select * from payment_history_18
    ) as table_union
    join product as pro
    on table_union.product_id = pro.product_number
    where customer_id in (
        select customer_id from table_tonghop
        where segment = 'Best Customers'
    )
    and message_id = 1
)
, table_count__order as (   
    select category,
        count(order_id) as [num_order]
    from table_joined
    group by category
)
select *,
    sum(num_order) over() as [total_order],
    format(cast([num_order] as float) / sum(num_order) over(), 'p') as [pct]
from table_count__order
order by num_order desc


-- Thời gian trung bình giữa các lần mua hàng(tính tổng của các khách hàng)
with table_joined as (
    select customer_id, order_id, transaction_date
    from (
        select * from payment_history_17
        union all select * from payment_history_18
    ) as table_union
    join product as pro
    on table_union.product_id = pro.product_number
    where customer_id in (
        select customer_id from table_tonghop
        where segment = 'Best Customers'
    )
    and message_id = 1
)
, table_num_day as (
    select *,
        lag(transaction_date, 1) over(partition by customer_id order by transaction_date asc) as [lag_transaction],
        datediff(day, lag(transaction_date, 1) over(partition by customer_id order by transaction_date asc), transaction_date) as [num_day_buy]
    from table_joined
)
, table_avg_day as (
    select customer_id,
        AVG(num_day_buy) as [avg_day]
    from table_num_day
    group by customer_id
)
select avg_day,
    count(customer_id) as [count]
from table_avg_day
group by avg_day
order by count(customer_id) desc



-- Tỉ trong các nhóm sản phẩm được mua
with table_joined as (
    select customer_id, order_id, product_group
    from (
        select * from payment_history_17
        union all select * from payment_history_18
    ) as table_union
    join product as pro
    on table_union.product_id = pro.product_number
    where customer_id in (
        select customer_id from table_tonghop
        where segment = 'Best Customers'
    )
    and message_id = 1
)
select product_group,
    count(order_id) as [count],
    sum(count(order_id)) over() as [total_order],
    format(cast(count(order_id) as float) / sum(count(order_id)) over(), 'p') as [percent]
from table_joined
group by product_group
order by count(order_id) desc


-- Tổng số tiền của các nhóm khách hàng
select segment,
    sum(cast(monetary as bigint)) as [total_price_by_seg],
    sum(sum(cast(monetary as bigint))) over () as [total_price],
    format(cast(sum(cast(monetary as bigint)) as float) / sum(sum(cast(monetary as bigint))) over (), 'p') [percent]
from table_tonghop
group by segment
order by  sum(monetary) desc


-- Tính retention rate
with table_joined as (
    select customer_id, transaction_date,
        min(transaction_date) over(partition by customer_id) as first_purchase_date,
        datediff(month, min(transaction_date) over(partition by customer_id), transaction_date) as month_n
    from (
        select * from payment_history_17
        union all
        select * from payment_history_18
    ) as table_union
    join product as pro
        on table_union.product_id = pro.product_number
    where customer_id in (
        select customer_id from table_tonghop
        where segment = 'Best Customers'
    )
    and message_id = 1
),
cohort_labeled as (
    select 
        customer_id,
        format(first_purchase_date, 'yyyy-MM') as cohort_month,
        month_n
    from table_joined
),
retention as (
    select 
        cohort_month, month_n,
        count(distinct customer_id) as retained_customers
    from cohort_labeled
    group by cohort_month, month_n
),
retention_with_rate as (
    select 
        cohort_month, month_n, retained_customers,
        max(case when month_n = 0 then retained_customers end) over(partition by cohort_month) as cohort_size
    from retention
)
select 
    cohort_month, month_n, retained_customers, cohort_size,
    format(cast(retained_customers as float) / cohort_size, 'p') as retention_rate
from retention_with_rate
order by cohort_month, month_n;

