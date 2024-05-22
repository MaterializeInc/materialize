#include <decimal.h>

extern int string2decimal(const char *from, decimal_t *to, const char **end);
extern int decimal2string(const decimal_t *from, char *to, int *to_len);
extern int decimal2bin(const decimal_t *from, uchar *to, int precision, int scale);
extern int bin2decimal(const uchar *from, decimal_t *to, int precision, int scale, bool keep_prec);
extern int decimal_bin_size(int precision, int scale);

extern "C"
{
    int c_string2decimal(const char *from, decimal_t *to, const char **end);
    int c_decimal2string(const decimal_t *from, char *to, int *to_len);
    int c_decimal2bin(const decimal_t *from, uchar *to, int precision, int scale);
    int c_bin2decimal(const uchar *from, decimal_t *to, int precision, int scale, bool keep_prec);
    int c_decimal_bin_size(int precision, int scale);
}