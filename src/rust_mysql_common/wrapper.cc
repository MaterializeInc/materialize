#include "wrapper.hh"

int c_string2decimal(const char *from, decimal_t *to, const char **end)
{
    return string2decimal(from, to, end);
}

int c_decimal2string(const decimal_t *from, char *to, int *to_len)
{
    return decimal2string(from, to, to_len);
}

int c_decimal2bin(const decimal_t *from, uchar *to, int precision, int scale)
{
    return decimal2bin(from, to, precision, scale);
}

int c_bin2decimal(const uchar *from, decimal_t *to, int precision, int scale, bool keep_prec)
{
    return bin2decimal(from, to, precision, scale, keep_prec);
}

int c_decimal_bin_size(int precision, int scale)
{
    return decimal_bin_size(precision, scale);
}