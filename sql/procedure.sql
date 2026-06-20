create procedure UpdateWatermarkTable
    @lastload varchar(200)
as 
begin 
    begin transaction;

    update watermark_table
    set last_load = @lastload

    commit transaction;
end;
