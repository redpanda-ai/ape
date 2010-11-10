import os, sys, operator, pyodbc, locale

locale.setlocale(locale.LC_ALL, 'en_US.utf8')

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

	for row in rows:
		lenrow = len(row)
		for i in range(lenrow):
			sys.stdout.write(str(row[i]) + "\t")
		sys.stdout.write("\n")

def insert_to_table(products):
	#assume for the moment that Catalog Numbers are unique
	#filter out uniques
	x = ('1234', 0, '"s"', '"Catalog Number"')
	uniqs = {}
	for d in products:
		if (str(d).find('"Catalog Number"') != -1):
			uniqs[d] = products[d]

	sorted_keys = sorted(uniqs.keys())
	params = []
	for k in sorted_keys:
		vendor_id, row_id, element_type_1, element_type_2 = \
			str(k)[1:-1].split(",")
		v = int(vendor_id.replace("'",""))
		element_type_1 = element_type_1.replace('"','').replace("'","")
		element_type_2 = element_type_2.replace('"','').replace("'","")
		#making tuples for pydobc executemany
		t = (v,row_id,element_type_1,element_type_2,uniqs[k].replace('"',""))
		params.append(t) 
	#print params
	print params
	command = " INSERT INTO LogShipping.dba_tools.product_candidates " + \
	"(vendor_id, row_id, element_type_1, element_type_2, unique_thing) " + \
	"values (?,?,?,?,?)"
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



def clean_up_dirty_text (string):
	result = ""
	for c in string: 
		if ord(c) < 0x80: result +=c
		elif ord(c) < 0xC0: result += ('\xC2' + c)
		else: result += ('\xC3' + chr(ord(c) - 64))
	return result

def make_rules(file_name):
	f = open(file_name, 'r')
	lines = f.readlines()
	f.close()
	key_line = lines.pop(0)
	rule_keys = key_line.strip().split('\t')
	for line in lines:
		v = line.strip().split('\t')
		rules[(v[0],v[1],v[2])] = (v[3],v[4])

def map_products(file_name,products):
	f = open(file_name, 'r')
	lines = f.readlines()
	f.close()
	key_line = lines.pop(0)
	#get a list of product keys (e.g. "Vendor Name", "VendorID")
	product_key_list = key_line.strip().split('\t')
	product_keys, product_vals = {}, {}
	#convert it into a dictionary
	for x in range(len(product_key_list)):
		product_keys[product_key_list[x]] = x
		product_vals[x] = product_key_list[x]
	vendor_id_pos = product_keys['"Vendor ID"']
	vendor_name_pos = product_keys['"Vendor Name"']

	line_number = 0
	for line in lines:
		#get the product values
		pv = line.strip().split('\t')
		c = 0
		vid = pv[vendor_id_pos]
		vname = pv[vendor_name_pos]
		for item in pv:
			t = (vname,vid,product_vals[c])
			if t in rules:
				products[vid,line_number,rules[t][0], rules[t][1]] = item
			c += 1
		line_number +=1


if len(sys.argv) != 4:
	print "usage: python " + sys.argv[0] + " <dsn> <sql_login> <sql_passwd>"
	sys.exit(0)
dsn,uid,pwd = sys.argv[1], sys.argv[2], sys.argv[3]

rule_keys, rules, products = [], {}, {}
make_rules("rules.tab")
map_products("product_file.tab",products)
insert_to_table(products)
fetch_from_table()
compare_to_database()

#print "PRODUCT CANDIDATES:"
#sorted_keys = sorted(products.keys())
#for k,v in products.iteritems():
#	print k,v
#for k in sorted_keys:
#	print k, products[k]
#print "RULES:"
#for k,v in rules.iteritems():
#	print k,v

