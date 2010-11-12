import os, sys, operator, pyodbc, locale, unicodedata

locale.setlocale(locale.LC_ALL, 'en_US.utf8')

def clean_up_dirty_text (string):
	result = ""
	for c in string: 
		if ord(c) < 0x80: result +=c
		elif ord(c) < 0xC0: result += ('\xC2' + c)
		else: result += ('\xC3' + chr(ord(c) - 64))
	return result

def get_lines_from_file(file_name):
	try:
		f = open(file_name, 'r')
		lines = f.readlines()
		f.close()
	except Exception:
		print "Unable to read from " + file_name + "\nAborting."
		sys.exit(0)
	return lines

def fetch_from_table():
	command = """
SELECT * 
FROM 
	LogShipping.dba_tools.product_candidates x
"""
	cnxn = pyodbc.connect('DSN='+dsn+';UID='+uid+';PWD='+pwd)
	cursor = cnxn.cursor()
	cursor.execute(command)
	rows = cursor.fetchall()
	cnxn.close()

	header,column_ordinal = '',0
	for column in cursor.description:
		header += "%s\t" % column[0]
		column_ordinal += 1	
	header = header[0:len(header)-1] + "\n"
	sys.stdout.write(header)
	for row in rows:
		lenrow = len(row)
		for i in range(lenrow):
			sys.stdout.write(str(row[i]) + "\t")
		sys.stdout.write("\n")

def truncate_table():
	command = "TRUNCATE TABLE LogShipping.dba_tools.product_candidates"
	print command
	cnxn = pyodbc.connect('DSN='+dsn+';UID='+uid+';PWD='+pwd)
	cursor = cnxn.cursor()
	cursor.execute(command)
	cnxn.commit()
	cnxn.close()

def insert_to_items(broken_ties):

	sorted_keys = sorted(broken_ties.keys())
	params = []
	for k in sorted_keys:
		t = (broken_ties[k][0],broken_ties[k][1],k)
		params.append(t) 
	command = """
INSERT INTO LogShipping.dba_tools.product_candidates
	(vendor_id, row_id, tie_breaker)
	values (?,?,?)
"""
	print command
	cnxn = pyodbc.connect('DSN='+dsn+';UID='+uid+';PWD='+pwd)
	cursor = cnxn.cursor()
	cursor.executemany(command,params)
	cnxn.commit()
	cnxn.close()

def compare_to_database():
	command = """
SELECT
	i.itemid item_id
	, cat_no.spec catalog_number_in_db
	, c.unique_thing
FROM
		bcproduct.dbo.item i WITH (NOLOCK)
		INNER JOIN biocompare.dbo.item_spec cat_no WITH (NOLOCK)
			ON i.itemid = cat_no.item_id
			AND cat_no.spec_type_id = 2
		INNER JOIN biocompare.dbo.item_vendor iv
			ON i.itemid = iv.item_id
			AND iv.vendor_id = 1234
		INNER JOIN LogShipping.dba_tools.product_candidates c
			ON cat_no.spec = c.unique_thing
"""
	cnxn = pyodbc.connect('DSN='+dsn+';UID='+uid+';PWD='+pwd)
	cursor = cnxn.cursor()
	cursor.execute(command)
	rows = cursor.fetchall()
	cnxn.close()

	column_ordinal, header = 0, ''
	for column in cursor.description:
		header += "%s," % column[0]
		column_ordinal += 1
		header = header[0:len(header)-1] + "\t"

	sys.stdout.write(header)
	for row in rows:
		lenrow = len(row)
		for i in range(lenrow):
			sys.stdout.write(str(row[i]) + "\t")
		sys.stdout.write("\n")

def make_rules(file_name):
	lines = get_lines_from_file(file_name)
	key_line = lines.pop(0)
	rule_keys = key_line.strip().split('\t')
	global tiebreaker
	for line in lines:
		v = line.strip().split('\t')
		if v[3] == 't': #write a tiebreaker
			tiebreaker = v
			print "Tiebreaker found -> " + tiebreaker[4]
		else: #write a rule
			rules[(v[0],v[1],v[2])] = (v[3],v[4])
	if tiebreaker == ():
		print "Error, No tiebreaker defined in " + file_name + "\nAborting."
		sys.exit(0)

def map_products(file_name,products,broken_ties,unbroken_ties):
	lines = get_lines_from_file(file_name)
	key_line = lines.pop(0)
	product_key_list = key_line.strip().split('\t')
	product_keys, product_vals = {}, {}
	t = tiebreaker[4].split('+')
	for x in range(len(product_key_list)):
		product_keys[product_key_list[x]] = x
		product_vals[x] = product_key_list[x]
	tiebreaker_locations = []
	for item in t:
		tiebreaker_locations.append(product_keys[item.strip()])

	vendor_id_pos = product_keys['Vendor ID']
	vendor_name_pos = product_keys['Vendor Name']

	line_number, found_matching_rule = 0,0
	for line in lines:
		c,product_values = 0,line.strip().split('\t')
		tiebreaker_value = ''
		for location in tiebreaker_locations:
			tiebreaker_value += product_values[location]

		vid = product_values[vendor_id_pos]
		vname = product_values[vendor_name_pos]
		if tiebreaker_value not in broken_ties:
			broken_ties[tiebreaker_value] = (vid,line_number)
		else:
			tup = (vid,line_number,tiebreaker_value)
			unbroken_ties.append(tup)
			#print "Duplicate tiebreaker value on row " + str(line_number)
		#broken_ties[vid,line_number] = tiebreaker_value

		for item in product_values:
			t = (vname,vid,product_vals[c])
			if t in rules:
				found_matching_rule +=1
				products[vid,line_number,rules[t][0], rules[t][1]] = item
			c += 1
		line_number +=1
	if (found_matching_rule == 0):
		sys.stdout.write("Matching rules not found!\n")
	if len(unbroken_ties) > 0:
		print "Your tiebreaker does not break all ties, please correct it"
		print "broken_ties -> " + str(len(broken_ties))
		print "unbroken_ties -> " + str(len(unbroken_ties))
		sys.exit(0)
	else:
		print "Your tiebreaker is successful"

def insert_to_attributes(products):
	print "Inserting Attributes"
	sorted_keys = sorted(products.keys())
	params = []
	for k in sorted_keys:
		t = (k[0],k[1],k[2],k[3],clean_up_dirty_text(products[k]).encode('ascii'))
		params.append(t) 
	command = """
INSERT INTO LogShipping.dba_tools.product_candidate_attributes
	(vendor_id, row_id, attribute_type, attribute_name, attribute_value)
	values (?,?,?,?,?)
"""
	print command
	print str(params)
	cnxn = pyodbc.connect('DSN='+dsn+';UID='+uid+';PWD='+pwd)
	cursor = cnxn.cursor()
	cursor.executemany(command,params)
	cnxn.commit()
	cnxn.close()

	

if len(sys.argv) != 6:
	print "usage: python " + sys.argv[0] + " <dsn> <sql_login> <sql_passwd> " + \
	"<rules_file> <product_file>"
	sys.exit(0)
dsn,uid,pwd,rules_file,product_file = sys.argv[1:6]

rule_keys, rules, tiebreaker, products, broken_ties = [], {}, (), {}, {}
unbroken_ties = []
make_rules(rules_file)
map_products(product_file,products,broken_ties,unbroken_ties)
truncate_table()
#insert_to_items(broken_ties)
insert_to_attributes(products)
#print str(products)
#fetch_from_table()
#compare_to_database()


