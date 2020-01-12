#ifndef MULTI_JOIN_PROJECTION_H
#define MULTI_JOIN_PROJECTION_H

typedef struct Projection* Projection_Ptr;

Projection_Ptr Allocate_Projection_Array(int num_of_projections);
Projection_Ptr Get_Proj_by_index(Projection_Ptr Proj_Array,int index);
//Projection Accessors//
int *Get_Projection_Relation(Projection_Ptr Projection);
int *Get_Projection_Column(Projection_Ptr Projection);

//Projection Mutators//
void Set_Projection_Relation(Projection_Ptr Projection,int rel);
void Set_Projection_Column(Projection_Ptr Projection,int col);

void Print_Projection(Projection_Ptr);

#endif //MULTI_JOIN_PROJECTION_H
