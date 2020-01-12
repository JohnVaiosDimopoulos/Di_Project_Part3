#ifndef SORT_JOIN_DATA_HISTOGRAM_H
#define SORT_JOIN_DATA_HISTOGRAM_H

#include <stdio.h>
#include <stdint.h>
#include "../Relation_Creator/Relation_Creator.h"
#define HISTOGRAM_SIZE 256


typedef struct Histogram* Histogram_Ptr;

int* Get_Histogram_Array(Histogram_Ptr);
Histogram_Ptr Get_Histogram(RelationPtr Relation, const int byte_to_check);
void Print_Histogram(Histogram_Ptr);
void Delete_Histogram(Histogram_Ptr);

#endif //SORT_JOIN_RELATION_CREATOR_H
