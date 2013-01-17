#ifndef CA_ERROR_H_
#define CA_ERROR_H_ 1

const char *
ca_last_error (void);

void
ca_clear_error (void);

void
ca_set_error (const char *format, ...);

#endif /* !CA_ERROR_H_ */
