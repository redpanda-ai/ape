#!/usr/bin/env ruby

require 'dbi'
require 'ostruct'

def foo()
	dsn, login, passwd, rules, pfile = ARGV[0],ARGV[1],ARGV[2],ARGV[3],ARGV[4]
	big_insert = ''
	source = File.new(pfile, "r")
	while line = source.gets
		fields = line.split("\t")
		field = fields[4].gsub("'","''")
		fields[4].gsub!("'","''")
		big_insert += "INSERT INTO LogShipping.dba_tools.ruby_test(data) " \
		"VALUES ('" + fields[4] +  "')\n"
	end
	source.close
	db = DBI.connect("dbi:ODBC:" + dsn, login, passwd)
	db.do(big_insert)
	db.disconnect
end

def make_rules(rules_file)
	lines = File.open(rules_file).collect
	key_line = lines.first
	lines.delete_at(0)
	rule_keys = key_line.split("\t")
	for line in lines do
		vals = line.split("\t")
		if vals[3] == "t"
			$tiebreaker = vals[4]
			puts "Tiebreaker found -> " + $tiebreaker
		else
			k,v = OpenStruct.new, OpenStruct.new
			k.vendor_name, k.vendor_id, k.field_name = vals[0], vals[1], vals[2]
			v.our_type, v.our_field_name = vals[3], vals[4]
			$rules[k] = v
		end
	end
	if $tiebreaker == ''
		puts "Tiebreaker not found in #{rules_file}, aborting."
		exit
	end
end

def crunch_product_file(products_file)
	#open file
	lines = File.open(products_file).collect
	key_line = lines.first
	lines.delete_at(0)
	product_key_list = key_line.split("\t")
	product_keys, product_vals = {}, {}
	#create two hashes, one of keys and the other of values
	for x in (0..product_key_list.length) do
		product_keys[product_key_list[x]] = x
		product_vals[x] = product_key_list[x]
	end
	return product_keys, product_vals, lines
end

def compose_tie_breaker(tiebreaker_locations,line,line_number,vendor_id_pos,
	vendor_name_pos)
	c, product_values = 0, line.strip.split("\t")
	#compose the tiebreaker values, they are in the product_values
	tiebreaker_value = ''
	for location in tiebreaker_locations do
		tiebreaker_value += product_values[location]
	end

	vendor_id = product_values[vendor_id_pos]
	vendor_name = product_values[vendor_name_pos]

	tc = OpenStruct.new
	tc.vendor_id, tc.line_number = vendor_id, line_number
	tc.tiebreaker = tiebreaker_value
	if $broken_ties.has_value?(tiebreaker_value)
		$unbroken_ties << tc
	else
		$broken_ties[tiebreaker_value] = tc
	end
	return vendor_id, vendor_name
end

def map_products(products_file)
	product_keys, product_vals, lines = crunch_product_file(products_file)

	t = $tiebreaker.split("+")
	tiebreaker_locations = []
	for item in t do tiebreaker_locations << product_keys[item.strip] end
	vendor_id_pos = product_keys["Vendor ID"]
	vendor_name_pos = product_keys["Vendor Name"]

	line_number, found_matching_rule = 0,0
	for line in lines do
		c, product_values = 0, line.strip.split("\t")
		vendor_id, vendor_name = compose_tie_breaker(tiebreaker_locations,line,
		line_number,vendor_id_pos, vendor_name_pos)
	#compose the tiebreaker values, they are in the product_values
		#tiebreaker_value = ''
		#for location in tiebreaker_locations do
		#	tiebreaker_value += product_values[location]
		#end

		#vendor_id = product_values[vendor_id_pos]
		#vendor_name = product_values[vendor_name_pos]

		#tc = OpenStruct.new
		#tc.vendor_id, tc.line_number = vendor_id, line_number
		#tc.tiebreaker = tiebreaker_value
		#if $broken_ties.has_value?(tiebreaker_value)
		#	$unbroken_ties << tc
		#else
		#	$broken_ties[tiebreaker_value] = tc
		#end

		for item in product_values do
			t = OpenStruct.new
			t.vendor_name, t.vendor_id = vendor_id, vendor_name
			t.field_name = product_vals[c]
			found_matching_rule += 1
			if $rules.has_value?(t)
				found_matching_rule +=1
			end
			c += 1
		end
		line_number += 1
	end

	if found_matching_rule == 0
		puts "Matching rules not found, aborting"
		exit
	end

	if $unbroken_ties.length > 0
		puts "Your tiebreaker does not break all ties, please correct it"
		puts "broken_ties -> " + $broken_ties.length.to_s()
		puts "unbroken_ties -> " + $unbroken_ties.length.to_s()
	else
		puts "Your tiebreaker is successful."
		puts "broken_ties -> " + $broken_ties.length.to_s()
	end
end

unless ARGV.length == 5
	puts "usage: #{$0} <dsn> <sql_login> <sql_passwd> <rules_file> " \
	"<product_file>"
	exit
end
dsn, login, passwd, rfile, pfile = ARGV[0],ARGV[1],ARGV[2],ARGV[3],ARGV[4]

#foo()
$rules, $products = {}, {}
$tiebreaker, $broken_ties, $unbroken_ties = '', {}, []
make_rules(rfile)
map_products(pfile)

#lines = File.open(products).collect

#select = db.prepare('SELECT TOP 10 * FROM bcproduct.dbo.item')
#puts "Number of rows inserted: #{rows}"
