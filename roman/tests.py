import unittest
import roman_numerals

class ToRomanKnown(unittest.TestCase):
	def test_known(self):
		self.assertEqual(roman_numerals.to_roman(5), "V")
		self.assertEqual(roman_numerals.to_roman(10), "X")

	def test_other(self):
		self.assertEqual(roman_numerals.to_roman(31), "XXXI")

	def test_outofrange(self):
		self.assertRaises(roman_numerals.OutOfRangeError, \
		roman_numerals.to_roman, 0)

	def test_negative(self):
		self.assertRaises(roman_numerals.OutOfRangeError, \
		roman_numerals.to_roman, -1)

if __name__ == '__main__':
	unittest.main()
