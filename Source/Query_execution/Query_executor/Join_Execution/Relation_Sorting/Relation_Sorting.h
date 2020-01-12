#ifndef SORT_JOIN_RELATION_SORTING_H
#define SORT_JOIN_RELATION_SORTING_H

#include "../../../../Basis_Structs/Relation.h"
#include "../Prefix_sum/Prefix_Sum.h"

void Copy_Relation(RelationPtr,RelationPtr,Psum_Ptr,const int);
void Sort(RelationPtr);

#endif //SORT_JOIN_RELATION_SORTING_H
