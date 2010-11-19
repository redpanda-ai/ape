#!/usr/bin/python
# coding=utf8
from ctypes import *

cdll.LoadLibrary("libc.so.6")
cdll.LoadLibrary("iconv_tool.so.1")
lib_c = CDLL("libc.so.6")
lib_j = CDLL("iconv_tool.so.1")

#s = "Trs"
s = "Comme ci, comme ça"
src = c_char_p(s)
dst = c_char_p(s)
conv_desc = lib_j.initialize()
src_to_dst = lib_j.src_to_dst
src_to_dst(conv_desc,src,dst)
src_len = c_int(lib_c.strlen(src))
dst_len = c_int(lib_c.strlen(dst))
print "SRC -> '" + str(src.value) + "'"
print "DST -> '" + str(dst.value) + "'"

#lib_c.free(src)

printf = lib_c.printf
printf.argtypes = [c_char_p]
#x = printf("%s",dst)
#print x
lib_j.finalize(conv_desc)

