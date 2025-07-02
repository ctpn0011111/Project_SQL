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
    count(customer_id) as [num_customers],
    sum(count(customer_id)) over() as [total_customers],
    format(cast(count(customer_id) as float) / sum(count(customer_id)) over(), 'p') as [percent]
from table_segment
group by segment
order by count(customer_id) desc


-- Top những sản phẩm cuối cùng mà khách hàng mua ?
with table_joined as (
    select customer_id, order_id, sub_category, transaction_date,
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
    count(customer_id) as [num_customers],
    sum(count(customer_id)) over() as [total_customers],
    format(cast(count(customer_id) as float) / sum(count(customer_id)) over(), 'p') as [percent]
from table_joined
where [rank_day] = 1 -- lấy giá trị ngày cuối cùng mua hàng
group by sub_category
order by  count(customer_id) desc


-- Tỉ lệ thanh tóa thành công và thất bại theo đơn hàng
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
)
, table_status as (
    select [description],
        case when [description] = 'Success' then 'Success' else 'Faild' end as [payment_status]
    from table_joined
)
select payment_status,
    count([description]) as[count_status],
    format(cast(count([description]) as float) / sum(count([description])) over(), 'p') as [percent]
from table_status
group by payment_status
order by count([description]) desc



select * from table_message