import os, sys, operator

def make_dictionary(file_name,rule_keys,rules):
	f = open(file_name, 'r')
	lines = f.readlines()
	f.close()
	key_line = lines.pop(0)
	rule_keys = key_line.strip().split('\t')
	for line in lines:
		v = line.strip().split('\t')
		rules[(v[0],v[1],v[2])] = (v[3],v[4])

def map_products(file_name,rule_keys,rules,products):
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

rule_keys, rules, products = [], {}, {}
make_dictionary("rules.tab",rule_keys,rules)
map_products("product_file.tab",rule_keys,rules,products)
print "PRODUCT CANDIDATES:"
sorted_keys = sorted(products.keys())
#for k,v in products.iteritems():
#	print k,v
for k in sorted_keys:
	print k, products[k]
#print "RULES:"
#for k,v in rules.iteritems():
#	print k,v

