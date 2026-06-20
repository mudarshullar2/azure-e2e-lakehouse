create table watermark_table
(
    last_load varchar(2000)
)

select min(Date_ID) from [dbo].[source_cars_data]
insert into [dbo].[watermark_table]
values('DT00000')

-- resetting watermark table for whatever reasons
-- update watermark_table set last_load = 'DT00000'
