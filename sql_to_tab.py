import os.path, sys, pyodbc, locale

locale.setlocale(locale.LC_ALL, 'en_US.utf8')

header = ""

def get_results ( ) :
	command = "EXEC " + procedure_name + " " + params  
	print command
	cnxn = pyodbc.connect('DSN=' + dsn + ';UID=' + uid + ';PWD=' + pwd)
	cursor = cnxn.cursor()
	cursor.execute(command)
	#produce a header for the first line of the output_file
	column_ordinal = 0
	global header
	for column in cursor.description:
		header += "%s\t" % column[0]
		column_ordinal += 1	
	header = header[0:len(header)-1] + "\n"
	#get the resultset data and place into rows list
	rows = cursor.fetchall()
	return rows

def clean_up_dirty_text (string):
	result = ""
	for c in string: 
		if ord(c) < 0x80: result +=c
		elif ord(c) < 0xC0: result += ('\xC2' + c)
		else: result += ('\xC3' + chr(ord(c) - 64))
	return result

def update_single_csv_file ( output_file ) :
	FILE = open(output_file, 'w')
	rows = get_results( )
	FILE.write(header)
	for row in rows:
		lenrow = len(row)  
		for i in range(lenrow-1):
			FILE.write(clean_up_dirty_text(str(row[i])) + "\t")
		FILE.write(str(row[lenrow-1]))
		FILE.write("\n")
	FILE.close()
	
if len(sys.argv) != 7:
	print "usage: python " + sys.argv[0] + " <dsn> <uid> <pwd> " + \
	"<procedure_name> <params> <output_file>"
	sys.exit(0)
dsn,uid,pwd,procedure_name,params,output_file = sys.argv[1:7]
print "dsn -> " + dsn
#uid,pwd
update_single_csv_file(output_file)
