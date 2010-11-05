import unittest
import ape

class t(unittest.TestCase):
	def test_is_template_present(self):
		self.assertRaises(ape.TemplateFileError, ape.open_template_file, \
		"template_file.tab")

if __name__ == '__main__':
	unittest.main()
