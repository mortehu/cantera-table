#ifndef STORAGE_CA_TABLE_ERROR_H_
#define STORAGE_CA_TABLE_ERROR_H_ 1

#ifdef __cplusplus
extern "C" {
#endif

const char* ca_last_error(void);

void ca_clear_error(void);

void ca_set_error(const char* format, ...);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // !STORAGE_CA_TABLE_ERROR_H_
