#ifndef SORT_JOIN_TUPLE_H
#define SORT_JOIN_TUPLE_H
#include <stdint.h>

struct Tuple{
  uint64_t element;
  uint64_t row_id;
};

typedef struct Tuple* Tuple_Ptr;

#endif //SORT_JOIN_TUPLE_H
