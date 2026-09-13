SELECT current_user, current_database(), inet_server_addr(), inet_server_port();



SELECT * FROM products;

SELECT title,final_price from products;

select * from products
where final_price >50;

select * from products 
where rating >= 4.5;

select * from products 
where stock < 20;


select * from products 
order by final_price desc;

select * from products 
order by final_price desc
limit 5;


select avg(final_price) as average_final_price
from products;


select sum(stock) as total_stock
from products;

select count(*) as total_products
from products;



select category,count(*) as product_count
from products
group by category;


select category,avg(price) as average_price
from products
group by category;

select category,avg(rating) as average_rating
from products
group by category
having avg(rating) >4;

SELECT title, price, "discountPercentage", final_price
FROM products
WHERE "discountPercentage" > 15
ORDER BY "discountPercentage" DESC;

select 
title,
final_price,
case
when final_price <20 then 'low price'
when final_price between 20 and 50 then 'medium price'
else 'high price '
end as price_category
from products;

select * from products 
where final_price = (select max(final_price) from products);

with expensive_products as (
select * from products
where final_price > 50
) select * from expensive_products


SELECT
    title,
    category,
    final_price,
    RANK() OVER (ORDER BY final_price DESC) AS price_rank
FROM products;


SELECT
    title,
    category,
    final_price,
    ROW_NUMBER() OVER (
        PARTITION BY category
        ORDER BY final_price DESC
    ) AS category_rank
FROM products;



































