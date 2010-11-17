#!/usr/bin/env ruby

require 'dbi'
db = DBI.connect('dbi:ODBC:sf-dev-db', 'jkey_sa', 'Mus3Musculus!')
select = db.prepare('SELECT TOP 10 * FROM bcproduct.dbo.item')
select.execute
while rec = select.fetch do
	puts rec.to_s
end
db.disconnect
