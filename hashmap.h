#ifndef CA_HASHMAP_H_
#define CA_HASHMAP_H_ 1

struct ca_hashmap_data
{
  char *key;
  union
    {
      void *pointer;
      int64_t integer;
    } value;
};

struct ca_hashmap
{
  struct ca_hashmap_data *nodes;
  size_t size;
  size_t capacity;
};


struct ca_hashmap *
ca_hashmap_create (size_t capacity);

void
ca_hashmap_free (struct ca_hashmap *map);

int
ca_hashmap_insert (struct ca_hashmap *map, const char *key, void *value);

int64_t
ca_hashmap_insert_int (struct ca_hashmap *map, const char *key, int64_t value);

void *
ca_hashmap_get (const struct ca_hashmap *map, const char *key);

int64_t *
ca_hashmap_get_int (const struct ca_hashmap *map, const char *key);

#endif /* !CA_HASHMAP_H_ */
