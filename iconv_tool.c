#include<stdio.h>
#include<stdlib.h>
#include<stdarg.h>
#include<errno.h>
#include<string.h>
#include<iconv.h>

const char * SRC_SET = "UTF-8";
const char * DST_SET = "UCS-2";

void
showhex (const char * what, const char * src, int src_len)
{
	int i;
	printf ("%s: ", what);
	for (i = 0; i < src_len; i++) {
		printf ("%02X", (unsigned char) src[i]);
		if (i < src_len - 1)
			printf (" ");
	}
	printf ("\n");
}

void
show_values (const char * before_after,
	const char * src_start, int src_len_start,
	const char * dst_start, int dst_len_start)
{
	printf("%s:\n", before_after);
	showhex("src string", src_start, src_len_start);
	showhex("dst string", dst_start, dst_len_start);
}

iconv_t
initialize (void)
{
	iconv_t conv_desc;
	conv_desc = iconv_open (DST_SET, SRC_SET);
	if ((int) conv_desc == -1) {
		if (errno == EINVAL) {
			fprintf (stderr,
				"Conversion from '%s' to '%s' is not supported.\n",
				SRC_SET, DST_SET);
		} else {
			fprintf (stderr, "Initialization error %s\n",
				strerror (errno));
		}
		exit(1);
	}
	return conv_desc;
}

int
src_to_dst (iconv_t conv_desc, char * src, char * dst)
{
	size_t iconv_value;
	size_t src_len;
	size_t dst_len;

	char * dst_start;
	const char * src_start;
	int src_len_start;
	int dst_len_start;

	src_len = strlen(src);
	if (!src_len) {
		fprintf (stderr, "Input string is empty\n");
		return (0);
	}

	dst_len = 2 * src_len;
	dst = calloc (dst_len, 1);
	src_len_start = src_len;

	dst_len_start = dst_len;
	dst_start = dst;
	src_start = src;

	//show_values("before", src_start, src_len_start, dst_start, dst_len_start);

	iconv_value = iconv (conv_desc, & src, & src_len, & dst, & dst_len);

	//show_values("after", src_start, src_len_start, dst_start, dst_len_start);
	return 0;
}

void
finalize (iconv_t conv_desc)
{
	int v;
	v = iconv_close (conv_desc);
	if (v != 0) {
		fprintf (stderr, "iconv_close failed: %s\n", strerror (errno));
		exit(1);
	}
}

int main()
{
	char * in_string = "Très";
	char * out_string;
	iconv_t conv_desc;

	conv_desc = initialize();
	src_to_dst(conv_desc, in_string, out_string);
	finalize (conv_desc);
	return 0;
}
