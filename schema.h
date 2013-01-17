#ifndef CA_SCHEMA_H_
#define CA_SCHEMA_H_ 1

#include "smalltable.h"

int
ca_schema_load (const char *path);

void
ca_schema_close (void);

void
ca_schema_add_table (const char *name,
                     struct ca_table_declaration *declaration);

void
ca_schema_show_tables (void);

struct table *
ca_schema_table_with_name (const char *name);

#endif /* !CA_SCHEMA_H_ */
