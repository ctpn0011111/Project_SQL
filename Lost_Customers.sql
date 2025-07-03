-- Lần cuối mua hàng cách đây bao lâu

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
        where segment = 'Lost Bad Customers'
    )
    and message_id = 1
)
, table_rank_day as (
    select customer_id,
        datediff(day, transaction_date, '2018-12-31') as [num_day],
        row_number() over(partition by customer_id order by  datediff(day, transaction_date, '2018-12-31') desc) as [rank_day]
    from table_joined
)
, table_segment as (
    select customer_id, [num_day],
        case when [num_day] <= 90 then '< 3 month'
            when [num_day] between 91 and 180 then '3 - 6 month'
            when [num_day] between 181 and 270 then '6 - 9 month'
            when [num_day] between 271 and 360 then '9 - 12 month'
            when [num_day] between 361 and 450 then '12 - 15 month'
            when [num_day] between 451 and 540 then '15 - 18 month'
            when [num_day] between 541 and 630 then '18 - 21 month'
            when [num_day] between 631 and 720 then '21 - 24 month'
            else '> 2 year'
        end as [segment]
    from table_rank_day
    where [rank_day] = 1
)
select segment,
    count(distinct customer_id) as [num_customers],
    sum(count(customer_id)) over() as [total_customers],
    format(cast(count(customer_id) as float) / sum(count(customer_id)) over(), 'p') as [percent]
from table_segment
group by segment
order by count(customer_id) desc


-- Top những sản phẩm cuối cùng mà khách hàng mua và số tiền lần thanh toán của nhóm sản phẩm nào cao nhất ?
with table_joined as (
    select customer_id, order_id, sub_category, transaction_date, final_price,
        row_number() over(partition by customer_id order by transaction_date desc) as [rank_day]
    from (
        select * from payment_history_17
        union all select * from payment_history_18
    ) as table_union
    join product as pro
    on table_union.product_id = pro.product_number
    where customer_id in (
        select customer_id from table_tonghop
        where segment = 'Lost Bad Customers'
    )
    and message_id = 1 
)
select top 5 sub_category,
    count(distinct customer_id) as [num_customers],
    sum(cast(final_price as bigint)) as [total_price_by_sub],
    sum(sum(cast(final_price as bigint))) over() as [total_price],
    format(cast(count(distinct customer_id) as float) / sum(count(distinct customer_id)) over(), 'p') as [percent_num_cus],
    format(cast(sum(cast(final_price as bigint)) as float) / sum(sum(cast(final_price as bigint))) over(), 'p') as [percent_price]
from table_joined
where [rank_day] = 1 -- lấy giá trị ngày cuối cùng mua hàng
group by sub_category
order by count(customer_id) desc


-- Tỉ lệ thanh tóa thành công và thất bại theo đơn hàng
with table_joined as (
    select customer_id, order_id, [description],  transaction_date, online_offline
    from (
        select * from payment_history_17
        union all select * from payment_history_18
    ) as table_union
    join product as pro
    on table_union.product_id = pro.product_number
    join table_message as mess
    on table_union.message_id = mess.message_id
    where customer_id in (
        select customer_id from table_tonghop
        where segment = 'Lost Bad Customers'
    )
)
, table_status as (
    select [description], online_offline,
        case when [description] = 'Success' then 'Success' else 'Faild' end as [payment_status]
    from table_joined
)
select online_offline, payment_status,
    count([description]) as[count_status],
    format(cast(count([description]) as float) / sum(count([description])) over(partition by payment_status), 'p') as [percent]
from table_status
group by online_offline, payment_status
order by payment_status


-- Xác định tỉ lệ thanh toán thất bại theo mô tả
with table_joined as (
    select customer_id, order_id, [description],  transaction_date
    from (
        select * from payment_history_17
        union all select * from payment_history_18
    ) as table_union
    join product as pro
    on table_union.product_id = pro.product_number
    join table_message as mess
    on table_union.message_id = mess.message_id
    where customer_id in (
        select customer_id from table_tonghop
        where segment = 'Lost Bad Customers'
    )
    and table_union.message_id != 1
)
select  [description],
    count(order_id) as [num_order],
    sum(count(order_id)) over() as [total_order],
    format(cast( count(order_id) as float) / sum(count(order_id)) over(), 'p') as [percent]
from table_joined
group by [description]
order by  count(order_id) desc


-- Retention rate
with table_joined as (
    select customer_id, order_id,  transaction_date,
        format(min(transaction_date) over(partition by customer_id), 'yyyy-MM') as first_month,
        datediff(month, min(transaction_date) over(partition by customer_id), transaction_date) as month_n
    from (
        select * from payment_history_17
        union all select * from payment_history_18
    ) as table_union
    join product as pro
    on table_union.product_id = pro.product_number
    join table_message as mess
    on table_union.message_id = mess.message_id
    where customer_id in (
        select customer_id from table_tonghop
        where segment = 'Lost Bad Customers'
    )
    and table_union.message_id = 1
)
, table_cohort as (
    select 
        first_month,
        month_n,
        count(distinct customer_id) as [retained_customers] 
    from table_joined
    group by first_month, month_n
)
select *,
    max(retained_customers) over(partition by first_month) as [max_retained_by_fmonth],
    format(cast(retained_customers as float) / max(retained_customers) over(partition by first_month), 'p') as [percent]
from table_cohort
order by first_month, month_n, retained_customers desc



-- Tính tỉ lệ số đơn hàng thanh toán theo phương thức online/offline theo từng tháng mua hàng của khách hàng đó
with table_joined as (
    select customer_id, order_id,  transaction_date, online_offline,
        format(min(transaction_date) over(partition by customer_id), 'yyyy-MM') as first_month,
        datediff(month, min(transaction_date) over(partition by customer_id), transaction_date) as month_n
    from (
        select * from payment_history_17
        union all select * from payment_history_18
    ) as table_union
    join product as pro
    on table_union.product_id = pro.product_number
    join table_message as mess
    on table_union.message_id = mess.message_id
    where customer_id in (
        select customer_id from table_tonghop
        where segment = 'Lost Bad Customers'
    )
    and table_union.message_id = 1
)
, table_cohort as (
    select 
        first_month,
        month_n,
        count(case when online_offline = 'Online' then online_offline end) as [num_onl],
        count(case when online_offline = 'Offline' then online_offline end) as [num_off],
        count(case when online_offline = 'Not payment' then online_offline end) as [num_not_payment]
    from table_joined
    group by first_month, month_n
)
select *,
    format(cast([num_onl] as float) / ([num_onl] + [num_off] + [num_not_payment]), 'p') as [pct_order_onl],
    format(cast([num_off] as float) / ([num_onl] + [num_off] + [num_not_payment]), 'p') as [pct_order_off],
    format(cast([num_not_payment] as float) / ([num_onl] + [num_off] + [num_not_payment]), 'p') as [pct_order_not_payment]
from table_cohort
order by first_month, month_n


-- Tổng số tiền thanh toán mà công ty thu được từ tập khách hàng Lost Bad Customers
with table_joined as (
    select customer_id, order_id, [final_price]
    from (
        select * from payment_history_17
        union all select * from payment_history_18
    ) as table_union
    join product as pro
    on table_union.product_id = pro.product_number
    join table_message as mess
    on table_union.message_id = mess.message_id
    where customer_id in (
        select customer_id from table_tonghop
        where segment = 'Lost Bad Customers'
    )
    and table_union.message_id != 1
)
, table_total_price as (
    select customer_id,
        count(order_id) as [num_order],
        sum(cast(final_price as bigint)) as [num_price]
    from table_joined
    group by customer_id
)
select distinct count(num_order) over() as [total_order],
    sum(num_price) over() [total_price],
    round(cast(sum(num_price) over() as float) / count(num_order) over(), 2) as [avg_price]
from table_total_price



-- Tổng đơn hàng có mã giảm giá và đơn hàng bình thường. Số tiền tương ứng với từng nhóm
with table_joined as (
    select customer_id, order_id, [promotion_id]
    from (
        select * from payment_history_17
        union all select * from payment_history_18
    ) as table_union
    join product as pro
    on table_union.product_id = pro.product_number
    join table_message as mess
    on table_union.message_id = mess.message_id
    where customer_id in (
        select customer_id from table_tonghop
        where segment = 'Lost Bad Customers'
    )
    and table_union.message_id != 1
)
, table_slicer_promotion as (
    select customer_id, 
        count(order_id) as [num_order],
        count(case when promotion_id <> '0' then order_id end) as [total_order_promotion],
        count(case when promotion_id = '0' then order_id end) as [total_order_normal]
    from table_joined
    group by customer_id
)
select distinct sum([num_order]) over() as [total_order],
    sum([total_order_promotion]) over() as [total_order_pro],
    sum([total_order_normal]) over() as [total_order_normal]
from table_slicer_promotion;



-- Tổng số tiền mà khách thanh toán theo từng nhóm (promotion, normal)
with table_joined as (
    select customer_id, order_id, [promotion_id], final_price
    from (
        select * from payment_history_17
        union all select * from payment_history_18
    ) as table_union
    join product as pro
    on table_union.product_id = pro.product_number
    join table_message as mess
    on table_union.message_id = mess.message_id
    where customer_id in (
        select customer_id from table_tonghop
        where segment = 'Lost Bad Customers'
    )
    and table_union.message_id != 1
)
, table_total_price as (
    select
        sum(case when promotion_id <> '0' then final_price end) as [total_price_promotion],
        sum(case when promotion_id = '0' then final_price end) as [total_price_normal]
    from table_joined
)
select 
    total_price_promotion,
    total_price_normal,
    format(cast([total_price_promotion] as float) / ([total_price_promotion] + [total_price_normal]), 'p') as [pct_promotion],
    format(cast([total_price_normal] as float) / ([total_price_promotion] + [total_price_normal]), 'p') as [pct_normal]
from table_total_price;
