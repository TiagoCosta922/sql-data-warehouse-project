/*

script purpose cretae a database called DataWarehouse after check if already exists, and add three schemas to that:

'bronze'
'silver'
'gold'

*/
use master;
go

if exist (select 1 from sys.databases where name = 'DataWarehouse')
begin
  alter database DataWarehouse set single_user with rollback immediate;
  drop database DataWarehouse
End;
go

create database DataWarehouse;
go

use DataWarehouse;
go


create schema bronze;
go

create schema silver;
go

create schema gold;
go
