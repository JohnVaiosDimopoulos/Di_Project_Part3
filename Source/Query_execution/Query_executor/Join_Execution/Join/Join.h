#ifndef SORT_JOIN_DATA_JOIN_H
#define SORT_JOIN_DATA_JOIN_H

#include <stdio.h>
#include <stdint.h>
#include "../Relation_Creator/Relation_Creator.h"
#include "../Results_List/Results_List.h"

struct Result_List * Execute_Join(RelationPtr Relation_A, RelationPtr Relation_B);

#endif //SORT_JOIN_RELATION_CREATOR_H
