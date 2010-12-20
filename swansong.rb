#!/usr/bin/env ruby

require 'dbi'
require 'ostruct'

def delete_old_attributes_by_vendor_id()
	#vendor_id = product_vals["Vendor ID"]
	#puts "Vendor ID -> #{vendor_id}"
	cmd = "
DELETE FROM LogShipping.dba_tools.product_candidate_attributes
WHERE vendor_id = #{$vendor_id.to_s().strip}"
	#puts cmd
	db = DBI.connect("dbi:ODBC:" + $dsn, $login, $passwd)
	db.do(cmd)
	db.disconnect
end

def insert_to_attributes()
	big_insert = ''
	for k,v in $product_attributes do
		big_insert += "INSERT INTO LogShipping.dba_tools." \
		"product_candidate_attributes(vendor_id, row_id, attribute_type," \
		"attribute_name, attribute_value) VALUES ('" \
		+ k.vid + "', '" + k.row_id + "', '" + k.attribute_type + "', '" \
		+ k.attribute_name + "', '" + v + "')\n"
	end
	db = DBI.connect("dbi:ODBC:" + $dsn, $login, $passwd)
	db.do(big_insert)
	db.disconnect
	puts "Spreadsheet uploaded complete"
end

def make_rules(rules_file)
	lines = File.open(rules_file).collect
	key_line = lines.first
	lines.delete_at(0)
	rule_keys = key_line.split("\t")
	for line in lines do
		vals = line.split("\t")
		if vals[3] == "t"
			$tiebreaker = vals[4].strip
		else
			k,v = OpenStruct.new, OpenStruct.new
			k.vendor_name, k.vendor_id, k.field_name = vals[0], vals[1], vals[2]
			v.our_type, v.our_field_name = vals[3], vals[4]
			if $vendor_id == 0
				$vendor_id = vals[1]
			end
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

def compose_tie_breaker(t_params)
	c = 0
	#compose the tiebreaker values
	tiebreaker_value = ''
	for location in t_params.locations do
		tiebreaker_value += t_params.product_values[location]
	end

	vendor = OpenStruct.new
	vendor.vid = t_params.product_values[t_params.vid_pos]
	vendor.name = t_params.product_values[t_params.name_pos]

	tc = OpenStruct.new
	tc.vendor_id, tc.line_number = vendor.vid, t_params.line_number
	tc.tiebreaker = tiebreaker_value
	#place the tiebreaker_candidate in one of two buckets
	if $broken_ties.has_key?(tiebreaker_value)
		$unbroken_ties << tc
	else
		$broken_ties[tiebreaker_value] = tc
	end
	return vendor
end

def confirm_spreadsheet_row_uniqueness(product_keys, product_vals, lines)

	t_params = OpenStruct.new
	t_params.vid_pos = product_keys["Vendor ID"]
	t_params.name_pos = product_keys["Vendor Name"]
	t_params.locations = []
	t = $tiebreaker.split("+")
	for item in t do
		t_params.locations << product_keys[item.strip]
	end #end for

	line_number, found_matching_rule = 0,0
	#generate tie breaker for each product 
	for line in lines do
		t_params.product_values = line.strip.split("\t")
		t_params.line_number = line_number
		vendor = compose_tie_breaker(t_params)
		line_number += 1
		#check tie breaker against rules
		c = 0
		for item in t_params.product_values do
			#construct a key that may be in $rules
			t = OpenStruct.new
			t.vendor_name, t.vendor_id = vendor.name, vendor.vid
			t.field_name = product_vals[c]
			#scan rules for key
			for k,v in $rules do
				if (k == t)
					found_matching_rule +=1
					x = OpenStruct.new
					x.vid = vendor.vid.to_s().strip
					x.row_id = t_params.line_number.to_s().strip
					x.attribute_type = v.our_type.to_s().strip
					x.attribute_name = v.our_field_name.to_s().gsub("'","''").strip
					$product_attributes[x] = item.to_s().gsub("'","''").strip
				end #end if
			end #end for
			c += 1
		end #end for
	end #end for

	if found_matching_rule == 0
		puts "Matching rules not found, aborting"
		exit
	end #end if

	if $unbroken_ties.length > 0
		filter = {}
		for item in $unbroken_ties do
			filter[item.tiebreaker] = item.tiebreaker
		end
		for k,v in filter do
			$unbroken_ties << $broken_ties.delete(k)
		end
			
		puts "Your tiebreaker, '#{$tiebreaker}', produced" \
		+ " #{$unbroken_ties.length.to_s()} unbroken ties."
		puts "Those rows will be reported as exceptions."
	else
		puts "Your tiebreaker is successful."
	end
		puts "#{$broken_ties.length.to_s()} spreadsheet rows will be uploaded."
end

def search_for_duplicates_in_database()
	puts "Searching for duplicates in the database"
	main_template = "
SELECT itemid, COUNT(0) cnt
FROM
bcproduct.dbo.item i WITH (NOLOCK)
INNER JOIN LogShipping.dba_tools.product_candidate_attributes a
	ON a.attribute_value = i.item
	AND a.attribute_type = 'i'
	AND a.attribute_name = 'Item Name' _tb_specs
INNER JOIN biocompare.dbo.item_vendor v
	ON i.itemid = v.item_id
	AND a.vendor_id = v.vendor_id
WHERE
	v.vendor_id = _vendor_id
GROUP BY itemid
HAVING COUNT(0) > 1"

	tb_template = "
-- tb spec
INNER JOIN LogShipping.dba_tools.product_candidate_attributes s_tc_a
	ON a.row_id = s_tc_a.row_id
	AND s_tc_a.attribute_type = 's'
	AND s_tc_a.attribute_name = 'tb'
INNER JOIN biocompare.dbo.item_spec s_tc_b WITH (NOLOCK)
	ON i.itemid = s_tc_b.item_id
	AND s_tc_a.attribute_value = s_tc_b.spec
INNER JOIN biocompare.dbo.item_spec_type s_tc_c WITH (NOLOCK)
	ON s_tc_b.spec_type_id = s_tc_c.spec_type_id
	AND s_tc_c.spec_type = s_tc_a.attribute_name"

	new_tiebreaker = $tiebreaker
	for k,v in $rules do
		new_tiebreaker.sub!(k.field_name,v.our_field_name)
		vendor_id = k.vendor_id
	end

	tb_clause,t,c = '', new_tiebreaker.split("+"), 0
	for item in t do
		tb_clause += tb_template.gsub(/tb/,item.strip).gsub(/tc/,c.to_s())
		c += 1
	end
	select_duplicates = main_template.gsub(/_tb_specs/,tb_clause)
	select_duplicates.gsub!(/_vendor_id/,vendor_id)
	db = DBI.connect("dbi:ODBC:" + $dsn, $login, $passwd)
	puts select_duplicates
	$database_duplicates = db.select_all(select_duplicates)
	db.disconnect
	dl = $database_duplicates.length
	if dl > 0
		puts "#{dl} duplicates found in database, reported as exceptions."
		puts $database_duplicates
	else
		puts "0 database duplicates, proceeding."
	end
end

def produce_insert_report()
	puts "Producing insert report."
	main_template = "
SELECT y.row_id
FROM
	LogShipping.dba_tools.product_candidate_attributes y
	LEFT JOIN 
(
SELECT a.row_id
FROM
bcproduct.dbo.item i WITH (NOLOCK)
INNER JOIN LogShipping.dba_tools.product_candidate_attributes a
	ON a.attribute_value = i.item
	AND a.attribute_type = 'i'
	AND a.attribute_name = 'Item Name' _tb_specs
INNER JOIN biocompare.dbo.item_vendor v
	ON i.itemid = v.item_id
	AND a.vendor_id = v.vendor_id
WHERE
	v.vendor_id = _vendor_id
) x
	ON y.row_id = x.row_id
WHERE
	y.attribute_type = 'i'
	AND x.row_id IS NULL"

	tb_template = "
-- tb spec
INNER JOIN LogShipping.dba_tools.product_candidate_attributes s_tc_a
	ON a.row_id = s_tc_a.row_id
	AND s_tc_a.attribute_type = 's'
	AND s_tc_a.attribute_name = 'tb'
INNER JOIN biocompare.dbo.item_spec s_tc_b WITH (NOLOCK)
	ON i.itemid = s_tc_b.item_id
	AND s_tc_a.attribute_value = s_tc_b.spec
INNER JOIN biocompare.dbo.item_spec_type s_tc_c WITH (NOLOCK)
	ON s_tc_b.spec_type_id = s_tc_c.spec_type_id
	AND s_tc_c.spec_type = s_tc_a.attribute_name"

	new_tiebreaker = $tiebreaker
	for k,v in $rules do
		new_tiebreaker.sub!(k.field_name,v.our_field_name)
		vendor_id = k.vendor_id
	end

	tb_clause,t,c = '', new_tiebreaker.split("+"), 0
	for item in t do
		tb_clause += tb_template.gsub(/tb/,item.strip).gsub(/tc/,c.to_s())
		c += 1
	end

	select_new_products = main_template.gsub(/_tb_specs/,tb_clause)
	select_new_products.gsub!(/_vendor_id/,vendor_id)
	db = DBI.connect("dbi:ODBC:" + $dsn, $login, $passwd)
	#puts select_new_products
	ic = db.select_all(select_new_products)
	db.disconnect
	# collect row_ids for insert_candidates
	insert_candidates = []
	for row in ic do
		insert_candidates << row[0]
	end
	#filter out exceptions
	exceptions = []
	for item in $unbroken_ties do
		exceptions << item.line_number
	end
	for item in exceptions do
		insert_candidates.delete(item)
	end
	#display filtered candidates
	#for item in insert_candidates do
	#	puts "item -> #{item}"
	#end
	icl = insert_candidates.length
	if icl > 0
		puts "#{icl} insert candidates found, adding to report"
	else
		puts "0 insert candidates found, proceeding."
	end
	#puts "produce_insert_report, BROKEN, not implemented"
end

def produce_exception_report()
	myfile = File.new("exception_report_#{$vendor_id.to_s().strip}.tab", "w")
	myfile.puts("Exception Report\n")
	myfile.puts("vendor_id\tline_number\ttiebreaker_value")
	for item in $unbroken_ties do
		#write item tab delimited
		myfile.puts("#{item.vendor_id}\t#{item.line_number}" \
		+ "\t#{item.tiebreaker}")
	end
	myfile.close
	#puts "produce_exception_report, BROKEN, not implemented"
end

def produce_update_report()
	puts "produce_update_report, BROKEN, not implemented"
	puts "this will DELETE each spec provided by the spreadsheet for the item"
	puts "and then INSERT the spec with the one provided for the item"
	
end

def produce_delete_report()
	puts "produce_delete_report, BROKEN, not implemented"
	puts "this will DELETE all specs and then all items found in the database"
	puts "that are not found in the spreadsheets"
end

#main program start
puts "Program starting"
unless ARGV.length == 5
	puts "usage: #{$0} <dsn> <sql_login> <sql_passwd> <rules_file> " \
	"<product_file>"
	exit
end
$dsn, $login, $passwd, $rfile, $pfile = ARGV[0],ARGV[1],ARGV[2],ARGV[3],ARGV[4]
$rules, $product_attributes = {}, {}
$tiebreaker, $broken_ties, $unbroken_ties = '', {}, []
$database_duplicates = []
$vendor_id = 0
make_rules($rfile)
product_keys, product_vals, lines = crunch_product_file($pfile)
confirm_spreadsheet_row_uniqueness(product_keys, product_vals, lines)
delete_old_attributes_by_vendor_id()
insert_to_attributes()
search_for_duplicates_in_database()
produce_exception_report()
produce_insert_report()
produce_update_report()
produce_delete_report()
puts "Program complete"
#main program finish
