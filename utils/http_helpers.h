#ifndef HTTP_HELPERS_H
#define HTTP_HELPERS_H

#include <curl/curl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void http_init(void);
void http_cleanup(void);

char *http_get(const char* url, char* params[], size_t params_count, struct curl_slist* headers);
char *http_post(const char* url, const char* data, const char* params[], size_t params_count, struct curl_slist* headers);
char *http_put(const char* url, const char* data, const char* params[], size_t params_count, struct curl_slist* headers);

struct curl_slist *add_header(struct curl_slist* headers,
                              const char* header_key,
                              const char* header_value);

#endif // HTTP_HELPERS_H
