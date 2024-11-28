#include "postgres.h"
#include "http_helpers.h"

struct Response {
    char *data;
    size_t size;
};

static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
    size_t real_size = size * nmemb;
    struct Response *resp = (struct Response *)userp;

    char *ptr = realloc(resp->data, resp->size + real_size + 1);
    if (ptr == NULL) {
        return 0; // Out of memory
    }

    resp->data = ptr;
    memcpy(&(resp->data[resp->size]), contents, real_size);
    resp->size += real_size;
    resp->data[resp->size] = 0;

    return real_size;
}

void http_init(void)
{
    curl_global_init(CURL_GLOBAL_DEFAULT);
}

void http_cleanup(void)
{
    curl_global_cleanup();
}

char *http_get(const char* url, char* params[], size_t params_count, struct curl_slist* headers) {
    CURL *curl;
    CURLcode res;
    struct Response response = { .data = NULL, .size = 0 };
    char full_url[10000];
    size_t i;

    // Initialize response data
    response.data = malloc(1);
    response.size = 0;

    // Build the full URL with parameters
    snprintf(full_url, sizeof(full_url), "%s", url);
    for (i = 0; i < params_count; ++i) {
        strcat(full_url, i == 0 ? "?" : "&");
        strcat(full_url, params[i]);
    }

    curl = curl_easy_init();
    if (curl) {
        curl_easy_setopt(curl, CURLOPT_URL, full_url);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&response);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

        res = curl_easy_perform(curl);
        if (res != CURLE_OK) {
            free(response.data);
        }

        curl_easy_cleanup(curl);
    }

    return response.data;
}

char *http_post(const char* url, const char* data, const char* params[], size_t params_count, struct curl_slist* headers)
{
    CURL *curl;
    CURLcode res;
    struct Response response = { .data = NULL, .size = 0 };
    char full_url[10000];
    int i;

    // Initialize response data
    response.data = malloc(1);
    response.size = 0;

    // Build the full URL with parameters
    snprintf(full_url, sizeof(full_url), "%s", url);
    for (i = 0; i < params_count; ++i) {
        strcat(full_url, i == 0 ? "?" : "&");
        strcat(full_url, params[i]);
    }

    curl = curl_easy_init();
    if (curl)
    {
        curl_easy_setopt(curl, CURLOPT_URL, full_url);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&response);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

        res = curl_easy_perform(curl);
        if (res != CURLE_OK)
        {
            free(response.data);
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("curl_easy_perform() failed: %s", curl_easy_strerror(res))));
        }

        curl_easy_cleanup(curl);
    }

    return response.data;
}

char *http_put(const char* url, const char* data, const char* params[], size_t params_count, struct curl_slist* headers)
{
    CURL *curl;
    CURLcode res;
    struct Response response = { .data = NULL, .size = 0 };
    char full_url[10000];
    int i;

    // Initialize response data
    response.data = malloc(1);
    response.size = 0;

    // Build the full URL with parameters
    snprintf(full_url, sizeof(full_url), "%s", url);
    for (i = 0; i < params_count; ++i) {
        strcat(full_url, i == 0 ? "?" : "&");
        strcat(full_url, params[i]);
    }

    curl = curl_easy_init();
    if (curl)
    {
        curl_easy_setopt(curl, CURLOPT_URL, full_url);
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&response);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

        res = curl_easy_perform(curl);
        if (res != CURLE_OK)
        {
            free(response.data);
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("curl_easy_perform() failed: %s", curl_easy_strerror(res))));
        }

        curl_easy_cleanup(curl);
    }

    return response.data;
}

struct curl_slist *add_header(struct curl_slist* headers, const char* header_key, const char* header_value)
{
    char header[256];  // Adjust size as needed
    snprintf(header, sizeof(header), "%s: %s", header_key, header_value);
    headers = curl_slist_append(headers, header);
    return headers;
}