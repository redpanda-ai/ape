class OutOfRangeError(Exception):
	pass

roman_numeral_map = (
    ('M', 1000),('CM', 900),
    ('D',  500),('CD', 400),
    ('C',  100),('XC', 90),
    ('L',  50),('XL', 40),
    ('X',  10),('IX', 9),
    ('V',  5),('IV', 4),
    ('I',  1))

def to_roman(x):
	output = ''
	if x < 1:
		raise OutOfRangeError
	for roman, normal in roman_numeral_map:
		while x - normal >= 0:
			output += roman
			x -= normal
	return output

