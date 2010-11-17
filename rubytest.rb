#!/usr/bin/env ruby

require 'dbi'
db = DBI.connect('dbi:ODBC:dsn', 'user', 'pass')
select = db.prepare('SELECT TOP 10 * FROM bcproduct.dbo.item')
select.execute
while rec = select.fetch do
	puts rec.to_s
end
db.disconnect
