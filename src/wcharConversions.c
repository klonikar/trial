#include <stdio.h>
#include <wchar.h>

wchar_t *convertToString(int i, wchar_t *buf, size_t bufSize)
{
    wchar_t format[] = {'%', 'd', '\0'};
    int ret = swprintf(buf, bufSize, format, i);
    return buf;
}

int convertToInt(const wchar_t *buf, int *retP)
{
    int i = -1;
    wchar_t format[] = {'%', 'd', '\0'};
    int numFieldsConverted = swscanf(buf, format, retP);
    return numFieldsConverted;
}

int main(char **argv)
{
    wchar_t buf[10], *retBuf = NULL;
    int i = 1012, ret = -1;
    wchar_t testBuf[] = {'1','2','3','4', '\0'};
    retBuf = convertToString(i, buf, sizeof(buf));
    ret = convertToInt(testBuf, &i);
    printf("converted string: %S, converted int: %d\n", buf, i);

    return 0;
}
