#!/usr/bin/env ruby

require 'dbi'
require 'ostruct'
require 'thread'

def delete_old_attributes_by_vendor_id()
	cmd = "
	DELETE FROM LogShipping.dba_tools.product_candidate_attributes
	WHERE vendor_id = #{$vendor_id.to_s().strip}"
	cmd = "
	TRUNCATE TABLE LogShipping.dba_tools.product_candidate_attributes"
#puts cmd
	db = DBI.connect("dbi:ODBC:" + $dsn, $login, $passwd)
db.do(cmd)
	db.disconnect
	end

def insert_to_attributes()
	big_insert = ''
	att_count = 0
	db = DBI.connect("dbi:ODBC:" + $dsn, $login, $passwd)
	for k,v in $attributes do
	att_count += 1

	big_insert += "-- " + att_count.to_s() + "\n" + \
	"INSERT INTO LogShipping.dba_tools." \
	"product_candidate_attributes(vendor_id, row_id, attribute_type," \
	"attribute_name, attribute_value) VALUES ('" \
	+ k.vid + "', '" + k.row_id + "', '" + k.attribute_type + "', '" \
	+ k.attribute_name + "', '" + v + "')\n"
	if (att_count % 20 == 0)
db.do(big_insert)
	big_insert = ''
	end
#going to use a Queue eventually for asynchronous upload
	end
	if big_insert != ''
db.do(big_insert)
	end
	db.disconnect
	puts "\nAttribute Count -> " + att_count.to_s() + "\n"
#puts big_insert
	puts "SUCCESS"
	end

def make_rules(rules_file)
#open the rules file and strip out the first line
	rows = File.open(rules_file).collect
rows.delete_at(0)

#this hash describes the column order for a rules file
	column = { "vendor_name"=>0, "vendor_id"=>1, "vendor_field"=>2,
		"our_type"=>3, "field_name"=>4 }

#match each row to each column and construct a hash of rules
		for item in rows do
		row = item.split("\t")
		if $vendor_id == 0
		$vendor_id = row[column["vendor_id"]]
		end
		case row[column["our_type"]].strip
		when "tiebreaker"
		$tiebreaker = row[column["field_name"]].strip
		when "alias" #aliases
		k = row[column["vendor_field"]]
		v = row[column["field_name"]]
		$aliases[k] = v.strip
		else
		k,v = OpenStruct.new, OpenStruct.new
#the first three values form a tuple that is the key 
		k.vendor_name, k.vendor_id, k.field_name = row[0..2]
#the next two values form a tuple that is the value
		v.our_type, v.our_field_name = row[3..4]
		$rules[k] = v
		end
		end
#print "RULES -> " + $rules.to_s()
#abort if a tiebreaker entry was not found
		if $tiebreaker == ''
		puts "ERROR: Tiebreaker not found in #{rules_file}, aborting."
		exit
		end

#print $aliases.to_s()

		end

def slice_product_file(products_file)
#this function slices a file into three pieces
#product_lines - the actual rows in the product data file
#product_keys - a hash of names to ordinals ("item_id" => 7)
#product_vals - a hash of ordinals to names (7 => "item_id")

	product_lines = File.open(products_file).collect
	columns = product_lines.first.split("\t")
product_lines.delete_at(0)
	product_keys, product_vals = {}, {}
#create two hashes, one of keys and the other of values
#this is to provide a reversible lookup of keys to values
	for x in (0..columns.length) do
	product_keys[columns[x]] = x
	product_vals[x] = columns[x]
	end
	return product_keys, product_vals, product_lines
	end

def compose_tie_breaker(tiebreaker_columns)
#compose the tiebreaker values
	tiebreaker_value = ''
	for location in tiebreaker_columns.locations do
	tiebreaker_value += tiebreaker_columns.current_row[location]
	end

	vendor = OpenStruct.new
	vendor.vid = tiebreaker_columns.current_row[tiebreaker_columns.vid_pos]
	vendor.name = tiebreaker_columns.current_row[tiebreaker_columns.name_pos]

	tc = OpenStruct.new
	tc.vendor_id = vendor.vid
	tc.line_number = tiebreaker_columns.current_line_number
	tc.tiebreaker = tiebreaker_value
#place the tiebreaker_candidate in one of two buckets
if $sheet_singletons.has_key?(tiebreaker_value)
	$sheet_dupes << tc
	else
	$sheet_singletons[tiebreaker_value] = tc
	end
	return vendor
	end

def confirm_product_line_uniqueness(product_keys, product_vals, product_lines)
# create a structure to store the locations of the tiebreaker columns
	tiebreaker_columns = OpenStruct.new
	tiebreaker_columns.vid_pos = product_keys["Vendor ID"]
	tiebreaker_columns.name_pos = product_keys["Vendor Name"]
	tiebreaker_columns.locations = []
# split the tiebreaker so that "a + b + c" -> [a, b, c]
	t = $tiebreaker.split("+")
# add the location of each column to the structure
	for item in t do
	tiebreaker_columns.locations << product_keys[item.strip]
	end

	line_number, found_matching_rule = 0,0
#scan each product_line
	for line in product_lines do
	tiebreaker_columns.current_row = line.strip.split("\t")
	tiebreaker_columns.current_line_number = line_number
#compose the actual tie_breaker value for each item
#this will record $sheet_dupes and $sheet_singletons
vendor = compose_tie_breaker(tiebreaker_columns)
#cross-reference line against rules, extracting attributes
	column = 0
	att_count = 0
	for item in tiebreaker_columns.current_row do
#construct a key that may be in $rules
	t = OpenStruct.new
	t.vendor_name, t.vendor_id = vendor.name, vendor.vid
	t.field_name = product_vals[column]
#scan rules for key
	for k,v in $rules do
if (k == t)
	found_matching_rule +=1
	x = OpenStruct.new
	x.vid = vendor.vid.to_s().strip
	x.row_id = line_number.to_s().strip
	x.attribute_type = v.our_type.to_s().strip
	x.attribute_name = v.our_field_name.to_s().gsub("'","''").strip
	$attributes[x] = item.to_s().gsub("'","''").strip
	end
	att_count+=1
	end
	column += 1
	end
	line_number += 1
	end
#print "\nAttribute count -> " + att_count.to_s() + "\n"

	if found_matching_rule == 0
	puts "Matching rules not found, aborting"
	exit
	end

	if $sheet_dupes.length > 0
	filter = {}
	for item in $sheet_dupes do
	filter[item.tiebreaker] = item.tiebreaker
	end
	for k,v in filter do
$sheet_dupes << $sheet_singletons.delete(k)
	end

len_ex = $sheet_dupes.length.to_s()
	puts "WARNING: Your tiebreaker, '#{$tiebreaker}', produced" \
	+ " #{len_ex} unbroken ties."
emit_exception_report(len_ex)
	else
	puts "SUCCESS: Your tiebreaker is successful."
	end
	puts "1. Loading #{$sheet_singletons.length.to_s()} spreadsheet rows " \
		+ "for comparison with pre-existing DB records."
		end

def search_for_duplicates_in_database()
	puts "2. Scanning DB with provided rules for uniqueness." 
	main_template = "
	SELECT itemid, COUNT(0) cnt
	FROM
	bcproduct.dbo.item i
	INNER JOIN LogShipping.dba_tools.product_candidate_attributes a
	ON a.attribute_value = i.item
	AND a.attribute_type = 'product_name'
	_tb_specs
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
	AND s_tc_a.attribute_type = 'spec'
	AND s_tc_a.attribute_name = 'tb'
	INNER JOIN biocompare.dbo.item_spec s_tc_b 
	ON i.itemid = s_tc_b.item_id
	AND s_tc_a.attribute_value = s_tc_b.spec
	INNER JOIN biocompare.dbo.item_spec_type s_tc_c 
	ON s_tc_b.spec_type_id = s_tc_c.spec_type_id
	AND s_tc_c.spec_type = s_tc_a.attribute_name"

	new_tiebreaker = $tiebreaker
	for k,v in $rules do
#print "\n" + k.to_s()
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
#puts select_duplicates
$db_dupes = db.select_all(select_duplicates)
	db.disconnect
	dl = $db_dupes.length
	if dl > 0
	puts "WARNING: There are #{dl} pre-existing records in the database " \
		+ "that are not unique according to the rules provided.  These will " \
		+ "be reported as exceptions."
		puts $db_dupes
		else
		puts "SUCCESS"
		end
		end

def produce_update_report()
	puts "5. Generating Update Report"
	#puts $cte_clause

	attribute_change_template ="
SELECT
	matched_items.row_id,
	matched_items.itemid matched_item_id,
	matched_items.attribute_type attribute_type,
	original_prices.price old_value,
	matched_items.attribute_value new_value,
	matched_items.item_name item_name
FROM
(
	SELECT a.row_id, i.itemid, i.item item_name,
		x.attribute_value, x.attribute_type
	FROM
	bcproduct.dbo.item i
	-- bind attributes to product by name
	INNER JOIN LogShipping.dba_tools.product_candidate_attributes a
		ON a.attribute_value = i.item
		AND a.attribute_type = 'product_name'
	INNER JOIN biocompare.dbo.item_vendor v
		ON i.itemid = v.item_id
		AND a.vendor_id = v.vendor_id
	-- bind attributes to product by tiebreakers values
__attribute_stuff
	-- this is the attribute to update
	-- INNER JOIN -> both SS and DB have attribute x
	__inner_or_left JOIN LogShipping.dba_tools.product_candidate_attributes x
		ON a.row_id = x.row_id
		AND x.attribute_type = 'price'
	WHERE
		v.vendor_id = __vendor_id
	) matched_items
LEFT JOIN 
(
	SELECT i.itemid, i.item, p.price
	FROM
	bcproduct.dbo.item i
	INNER JOIN biocompare.dbo.item_price p
		ON i.itemid = p.item_id
		AND p.enabled = 1
) original_prices
	ON matched_items.itemid = original_prices.itemid
WHERE 
	original_prices.price != CAST(matched_items.attribute_value AS SMALLMONEY)
	OR ( (matched_items.attribute_value IS NOT NULL)
		AND (original_prices.price IS NULL) )
ORDER BY matched_items.row_id
"

	cte_template = \
"__with_or_comma s___tc_item_spec (item_id, spec, spec_type_id)
AS (
SELECT i.itemid, a.spec, b.spec_type_id
FROM
bcproduct.dbo.item i
INNER JOIN biocompare.dbo.item_vendor v
	ON i.itemid = v.item_id
	AND v.vendor_id = __vendor_id
INNER JOIN biocompare.dbo.item_spec a
	ON i.itemid = a.item_id
INNER JOIN biocompare.dbo.item_spec_type b
	ON a.spec_type_id = b.spec_type_id
where spec_type = '__spec_type'
)
"

	attribute_template = \
"	-- __spec_type
	INNER JOIN LogShipping.dba_tools.product_candidate_attributes s___tc_a
		ON a.row_id = s___tc_a.row_id
		AND s___tc_a.attribute_type = 'spec'
		AND s___tc_a.attribute_name = '__spec_type'
	INNER JOIN s___tc_item_spec s___tc_item_spec
		ON i.itemid = s___tc_item_spec.item_id
		AND s___tc_a.attribute_value = s___tc_item_spec.spec
	INNER JOIN biocompare.dbo.item_spec_type s___tc_c
		ON s___tc_item_spec.spec_type_id = s___tc_c.spec_type_id
		AND s___tc_c.spec_type = s___tc_a.attribute_name
"

	new_tiebreaker = $tiebreaker
	for k,v in $rules do
		new_tiebreaker.sub!(k.field_name,v.our_field_name)
		vendor_id = k.vendor_id
	end
	attribute_clause, cte_clause = '',''
	t,c = new_tiebreaker.split("+"), 0
	for item in t do
		if c == 0
			foo = cte_template.gsub(/__with_or_comma/,"WITH")
		else
			foo = cte_template.gsub(/__with_or_comma/,",")
		end
		foo.gsub!(/__tc/,c.to_s())
		foo.gsub!(/__vendor_id/,$vendor_id)
		foo.gsub!(/__spec_type/,item.strip)
		cte_clause += foo
		bar = attribute_template.gsub(/__tc/,c.to_s())
		bar.gsub!(/__spec_type/,item.strip)
		attribute_clause += bar
		c += 1
	end
	attribute_change_clause = \
		attribute_change_template.gsub(/__inner_or_left/,"INNER")

	attribute_change_clause.gsub!(/__vendor_id/,$vendor_id)
	attribute_change_clause.gsub!(/__attribute_stuff/,attribute_clause)

#	insert_attribute_clause = \
#		attribute_change_template.gsub(/__inner_or_left/,"LEFT")
#	insert_attribute_clause.gsub!(/__vendor_id/,$vendor_id)
#	insert_attribute_clause.gsub!(/__attribute_stuff/,attribute_clause)

	change_prices = cte_clause + attribute_change_clause
#	insert_prices = cte_clause + insert_attribute_clause

	#UPDATE PRICE
	db = DBI.connect("dbi:ODBC:" + $dsn, $login, $passwd)
	up = db.select_all(change_prices)
	db.disconnect
	# collect row_ids for insert_candidates
	# TO FIX: we don't need both $disable_candidates AND $items_to_disable :)
	$update_price_array, $update_price_hash = [], {}
	for row in up do
		$update_price_array << row[0]
		$update_price_hash[row[0]] = row[0].to_s() + "\t" + row[1].to_s() + \
		 "\t" + row[2].to_s() + "\t" + row[3].to_s() + "\t" + row[4].to_s() + \
		 "\t" + row[5].to_s()
	end

	#filter out exceptions would go here, if we used them

	puts change_prices

	upl = $update_price_array.length
	# emit update report
	puts "EMIT: update_attribute_report_#{$vendor_id.to_s().strip}.tab " \
	+ "(#{upl} items)"
	myfile = File.new("update_attribute_report_#{$vendor_id.to_s().strip}.tab" \
		, "w")
	myfile.puts("Update Attribute Report\n")
	myfile.puts("row_id\tmatched_item_id\tattribute_type\told_value" \
		+ "\tnew_value" + "\titem_name")
	for candidate in $update_price_array do
		myfile.puts($update_price_hash[candidate])
	end
	myfile.close

	#filter out exceptions
=begin
	exceptions = []
	for item in $db_dupes do
		exceptions << item.line_number
	end
	for item in exceptions do
		update_price_array.delete(item)
		update_price_hash.delete(item)
	end
=end
	puts "SUCCESS"

end

def produce_disable_report()
	puts "4. Generating disable report."

	disable_template_2 = "
SELECT itemid
FROM
(
SELECT
	-- i.itemid item_id_to_disable
	all_items.itemid bar
	, bound_items.itemid itemid
FROM
	( 
	SELECT i.itemid
	FROM
		bcproduct.dbo.item i
	INNER JOIN LogShipping.dba_tools.product_candidate_attributes a
		ON a.attribute_value = i.item
		AND a.attribute_type = 'product_name'
	INNER JOIN biocompare.dbo.item_vendor v
		ON i.itemid = v.item_id
		AND a.vendor_id = v.vendor_id
_tb_specs
	WHERE
		v.vendor_id = __vendor_id
	) bound_items
LEFT JOIN
	(
	SELECT i.itemid
	FROM
	bcproduct.dbo.item i
	INNER JOIN biocompare.dbo.item_vendor v
		ON i.itemid = v.item_id
	WHERE 
		v.vendor_id = 1234
		AND i.enabled = 1
	) all_items
ON bound_items.itemid = all_items.itemid
) x
WHERE x.bar IS NULL

"

	disable_template = "
SELECT
	i.itemid item_id_to_disable
	, cat_no.spec catalog_number
	, i.item item_name
FROM
	bcproduct.dbo.item i
	INNER JOIN biocompare.dbo.item_vendor v
		ON i.itemid = v.item_id
	INNER JOIN biocompare.dbo.item_spec cat_no
		ON i.itemid = cat_no.item_id
	INNER JOIN biocompare.dbo.item_spec_type t
		ON cat_no.spec_type_id = t.spec_type_id
		AND t.spec_type = 'Catalog Number'
	LEFT JOIN 
	( 
	SELECT i.itemid
	FROM
		bcproduct.dbo.item i
	INNER JOIN LogShipping.dba_tools.product_candidate_attributes a
		ON a.attribute_value = i.item
		AND a.attribute_type = 'product_name'
_tb_specs
	INNER JOIN biocompare.dbo.item_vendor v
		ON i.itemid = v.item_id
		AND a.vendor_id = v.vendor_id
	WHERE
		v.vendor_id = __vendor_id
	) bound_items
		ON i.itemid = bound_items.itemid
WHERE
	v.vendor_id = __vendor_id
	 AND bound_items.itemid IS NULL
	 AND i.enabled = 1
"
	tb_template = "
-- tb spec
INNER JOIN LogShipping.dba_tools.product_candidate_attributes s_tc_a
	ON a.row_id = s_tc_a.row_id
	AND s_tc_a.attribute_type = 'spec'
	AND s_tc_a.attribute_name = 'tb'
INNER JOIN biocompare.dbo.item_spec s_tc_b
	ON i.itemid = s_tc_b.item_id
	AND s_tc_a.attribute_value = s_tc_b.spec
INNER JOIN biocompare.dbo.item_spec_type s_tc_c
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

	disable_old_products = disable_template.gsub(/_tb_specs/,tb_clause)
	disable_old_products.gsub!(/__vendor_id/,$vendor_id)
	#puts disable_old_products
	db = DBI.connect("dbi:ODBC:" + $dsn, $login, $passwd)
	dc = db.select_all(disable_old_products)
	db.disconnect
	# collect row_ids for insert_candidates
	# TO FIX: we don't need both $disable_candidates AND $items_to_disable :)
	for row in dc do
		$disable_candidates << row[0]
		$items_to_disable[row[0]] = row[0].to_s() + "\t" + row[1].to_s() + \
		 "\t" + row[2].to_s()
	end
	#filter out exceptions
	exceptions = []
	for item in $db_dupes do
		exceptions << item.line_number
	end
	for item in exceptions do
		$disable_candidates.delete(item)
		$items_to_disable.delete(item)
	end
	dcl = $disable_candidates.length
	puts "SUCCESS"

	puts "EMIT: disable_report_#{$vendor_id.to_s().strip}.tab (#{dcl} items)"
	myfile = File.new("disable_report_#{$vendor_id.to_s().strip}.tab", "w")
	myfile.puts("Disable Report\n")
	myfile.puts("item_id\tcatalog_number\titem_name")
	#foo = disable_candidates.values()
	for candidate in $disable_candidates do
		myfile.puts($items_to_disable[candidate])
	end
	#myfile.puts(foo.sort)
	myfile.close
end

def produce_update_sql()
	puts "\nproduce_update_sql not implemented"
end

def produce_insert_report()
	puts "3. Generating insert report."
	main_template = "
SELECT y.row_id
FROM
	LogShipping.dba_tools.product_candidate_attributes y
	LEFT JOIN 
(
SELECT a.row_id
FROM
bcproduct.dbo.item i
INNER JOIN LogShipping.dba_tools.product_candidate_attributes a
	ON a.attribute_value = i.item
	AND a.attribute_type = 'product_name'
_tb_specs
INNER JOIN biocompare.dbo.item_vendor v
	ON i.itemid = v.item_id
	AND a.vendor_id = v.vendor_id
WHERE
	v.vendor_id = _vendor_id
) x
	ON y.row_id = x.row_id
WHERE
	y.attribute_type = 'product_name'
	AND x.row_id IS NULL"


	tb_template = "
-- tb spec
INNER JOIN LogShipping.dba_tools.product_candidate_attributes s_tc_a
	ON a.row_id = s_tc_a.row_id
	AND s_tc_a.attribute_type = 'spec'
	AND s_tc_a.attribute_name = 'tb'
INNER JOIN biocompare.dbo.item_spec s_tc_b
	ON i.itemid = s_tc_b.item_id
	AND s_tc_a.attribute_value = s_tc_b.spec
INNER JOIN biocompare.dbo.item_spec_type s_tc_c
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
	ic = db.select_all(select_new_products)
	db.disconnect
	# collect row_ids for insert_candidates
	insert_candidates = []
	for row in ic do
		insert_candidates << row[0]
	end
	#filter out exceptions
	exceptions = []
	for item in $sheet_dupes do
		exceptions << item.line_number
	end
	for item in exceptions do
		insert_candidates.delete(item)
	end
	icl = insert_candidates.length
	puts "SUCCESS"

insert_statements = {}
#used to dictate the order of the sql statements, lower numbers come first
sql_sort_order = {
	"product_name" => "1",
	"price" => "2",
	"spec" => "3",
	"category" => "4",
	"category_group_option" => "5"
}
	#place the insertable_attributes in their own hash
	for k,v in $attributes do
		for item in insert_candidates do
			if k.row_id == item.to_s()
				case k.attribute_type
					when "category", "category_group_option"
						if $aliases.has_key?(v)
							v=$aliases[v]
						end
				end
				$insertable_attributes[k] = k.vid + "\t" + k.row_id \
				+ "\t" + sql_sort_order[k.attribute_type] + k.attribute_type \
				+ "\t" + k.attribute_name + "\t" + v
				#print "\n" + $insertable_attributes[k]
			end
		end
	end
	puts "EMIT: insert_report_#{$vendor_id.to_s().strip}.tab (#{icl} items)"

	myfile = File.new("insert_report_#{$vendor_id.to_s().strip}.tab", "w")
	myfile.puts("Insert Report\n")
	myfile.puts("vendor_id\trow_id\tattribute_type\tattribute_name\tvalue")
	foo = $insertable_attributes.values()
	myfile.puts(foo.sort)
	myfile.close
end

def produce_disable_sql()
template = "UPDATE bcproduct.dbo.item SET enabled = 0 WHERE itemid = __itemid"
	#sorted_item_attributes = $insertable_attributes.values().sort
	puts "EMIT: disable_sql_#{$vendor_id.to_s().strip}.sql"
	myfile = File.new("disable_sql_#{$vendor_id.to_s().strip}.sql", "w")
	myfile.puts("-- DISABLES THE FOLLOWING")
	for item_id in $disable_candidates do
		x = template.gsub("__itemid",item_id.to_s())
		myfile.puts(x)
	end
	myfile.close()
end

def produce_insert_sql()

templates = {
	"product_name" =>
"
-- ROW #__row_id - __type
INSERT INTO bcproduct.dbo.item( m_itemid, item, image, antibody, bg_enabled,
	compressed, compressed_key, searchable, enabled, retired, modified)
	VALUES ( 0,
	'__value',
	0, 0, 1,
	1, 1, 0, 0, 0, @now )
SELECT @item_id = SCOPE_IDENTITY()

-- ROW #__row_id - item_vendor
INSERT INTO biocompare.dbo.item_vendor(item_id,vendor_id,is_targeted,modified,
	created,is_manufacturer,country_id)
	VALUES(@item_id,__vendor_id,0,@now,@now,0,219)
",
	"spec" =>
"
-- ROW #__row_id - __type
INSERT INTO biocompare.dbo.item_spec ( item_id, spec_type_id, vendor_id,
	country_id, spec, modified, created, matrix_display, display, enabled)
	SELECT
		@item_id, t.spec_type_id, __vendor_id, 0,
		'__value',
		@now, @now, 0, 0, 0
	FROM
		biocompare.dbo.item_spec_type t
	WHERE
		t.spec_type = '__key'
",
	"price" =>
"
-- ROW #__row_id - __type (USD)
INSERT INTO biocompare.dbo.item_price ( item_id, country_id, vendor_id, 
	currency_type_id, price, enabled, created, modified )
	VALUES( @item_id, 219, __vendor_id, 1, __value, 1, @now, @now)
",
	"category" =>
"
-- ROW #__row_id - __type
SELECT @category_id = category_id
	FROM biocompare.dbo.category WHERE category_name = '__value'
INSERT INTO biocompare.dbo.item_to_category ( item_id, category_id, 
	is_product_directory_enabled, enabled, created, modified)
	VALUES (@item_id, @category_id, 1, 1, @now, @now)
",
	"category_group_option" =>
"
-- ROW #__row_id - __type
SELECT @category_group_id = category_group_id
	FROM biocompare.dbo.category_group
	WHERE category_group = '__key'

SELECT @category_option_id = category_option
	FROM biocompare.dbo.category_group_option_id
	WHERE category_option = '__value'

SELECT @category_group_option_id = category_group_option_pk
	FROM biocompare.dbo.category_group_option
	WHERE 
		category_option_id = @category_option_id
		AND category_id = @category_id
		AND category_group_id = @category_group_id

INSERT INTO item_to_category_group_option( item_id, category_id,
	category_group_id, category_group_option_id, enabled, modified, created )
	SELECT @item_id, @last_category_id, @category_group_id,
	, @category_group_option_id, 1, @now, @now)
",
	"header" =>
"
DECLARE @item_id INT, @now DATETIME
DECLARE @category_id INT, @category_group_id INT
DECLARE @category_option_id INT
DECLARE @category_group_option_id INT
SELECT @now = GETUTCDATE()
"
}

#REGEX
	sorted_item_attributes = $insertable_attributes.values().sort
	puts "EMIT: insert_sql_#{$vendor_id.to_s().strip}.sql"
	myfile = File.new("insert_sql_#{$vendor_id.to_s().strip}.sql", "w")
	myfile.puts(templates["header"])
	pattern = /(\d*)\t(\d*)\t\d([^\t]*)\t([^\t]*)\t([^\t]*).*/
	for item in sorted_item_attributes do
		#print item
		arr = item.scan(pattern)[0]
		vendor_id,row_id,type,key,value = arr[0..4]
		if templates.has_key?(type)
			x = templates[type].gsub("__row_id",row_id)
			x.sub!("__value",value)
			x.sub!("__vendor_id",vendor_id)
			x.sub!("__key",key)
			x.sub!("__type",type)
			myfile.puts(x)
		else
			print "Type '" + type + "' was provided but has no template!"
		end
	end
end

def emit_exception_report(len_ex)
#emits a report of spreadsheet items that are NOT UNIQUE via the $tiebreaker
	puts "EMIT: exception_report_#{$vendor_id.to_s().strip}.tab "\
	+ "(#{len_ex} items)"
	myfile = File.new("exception_report_#{$vendor_id.to_s().strip}.tab", "w")
	myfile.puts("Exception Report\n")
	myfile.puts("vendor_id\tline_number\ttiebreaker_value")
	for item in $sheet_dupes do
		myfile.puts("#{item.vendor_id}\t#{item.line_number}" \
		+ "\t#{item.tiebreaker}")
	end
	myfile.close
end

#SIMPLE VARIALBES, FOUND IN THE RULES FILE
#$tiebreaker - uses form of "x + y + z" to indicate which fields from the
#product file uniquely identify an item
#$vendor_id - the id number of the vendor
$tiebreaker, $cte_clause, $vendor_id = '', '', 0

# HASHTABLES
#$rules - holds rules describing which product file columns match what item
#attributes
#$attributes - formed by applying $rules over the product file to find actual 
#attributes per item (e.g. item_name, price, Reactivity...)
#$sheet_singletons - holds products identified as unique by the $tiebreaker
#$insertable_attributes - subset of $attributes remaining after removing pre-existing DB items 
#$aliases - a simple dictionary that translates a vendor's words to one of 
#ours, made from $rules
$rules, $attributes = {}, {}
$sheet_singletons, $insertable_attributes, $aliases = {}, {}, {}
$items_to_disable = {}
$update_price_array, $update_price_hash = [], {}

#ARRAYS
#$sheet_dupes - holds any SPREADSHEET items that are not unique via $tiebreaker
#$db_dupes - holds any DATABASE products that are not unqiue via $tiebreaker
#$disable_candidates - holds DATABASE items that should be disabled
$sheet_dupes, $db_dupes, $disable_candidates = [], [], []

#COMMAND LINE ARGUMENTS
unless ARGV.length == 5
	puts "usage: #{$0} <dsn> <sql_login> <sql_passwd> <rules_file> " \
	"<product_file>"
	exit
end
$dsn, $login, $passwd, $rfile, $pfile = ARGV[0..4]

#MAIN PROGRAM
puts "-- PROGRAM BEGINS --"
make_rules($rfile)
product_keys, product_vals, product_lines = slice_product_file($pfile)
confirm_product_line_uniqueness(product_keys, product_vals, product_lines)
delete_old_attributes_by_vendor_id() #used for testing only
insert_to_attributes()
search_for_duplicates_in_database()
produce_insert_report()
produce_insert_sql()
produce_disable_report()
produce_disable_sql()
produce_update_report()
produce_update_sql()
puts "-- PROGRAM COMPLETE --"
