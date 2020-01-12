#include "Projection.h"
#include <stdlib.h>
#include <stdio.h>

struct Projection{
  int rel;
  int col;
};

Projection_Ptr Get_Proj_by_index(Projection_Ptr Proj_Array,int index){
  return &Proj_Array[index];
}

Projection_Ptr Allocate_Projection_Array(int num_of_projections){
  Projection_Ptr proj = (Projection_Ptr)malloc(num_of_projections * sizeof(struct Projection));
  return proj;
}

int *Get_Projection_Relation(Projection_Ptr Projection){
  return &(Projection->rel);
}

int *Get_Projection_Column(Projection_Ptr Projection){
  return &(Projection->col);
}

void Set_Projection_Relation(Projection_Ptr Projection,int rel){
  Projection->rel=rel;
}

void Set_Projection_Column(Projection_Ptr Projection,int col){
  Projection->col=col;
}

void Print_Projection(Projection_Ptr Projection){
  printf(" %d.%d\t", Projection->rel,Projection->col);

}
