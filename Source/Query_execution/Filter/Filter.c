#include "Filter.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

struct Filter{
  int rel;
  int col;
  char type[2];
  int constant;
};

Filter_Ptr Allocate_filter_Array(int num_of_filters){
  Filter_Ptr Filter_Array = (Filter_Ptr)malloc(num_of_filters * sizeof(struct Filter));
  return Filter_Array;
}

Filter_Ptr Get_Filter_by_index(Filter_Ptr Filter_array,int index){
  return &(Filter_array[index]);
}

//Filter Accessors//

int Get_Filter_Relation(Filter_Ptr Filter){
  return Filter->rel;
}

int Get_Filter_Column(Filter_Ptr Filter){
  return  Filter->col;
}

int Get_Constant(Filter_Ptr Filter){
  return Filter->constant;
}

char* Get_Type(Filter_Ptr Filter){
  return Filter->type;
}

//Filter Mutators//

void Set_Relation(Filter_Ptr Filter,int rel){
  Filter->rel=rel;
}

void Set_Column(Filter_Ptr Filter,int col){
  Filter->col=col;
}

void Set_type(Filter_Ptr Filter,char type[2]){
  strcpy(Filter->type,type);
}

void Set_constant(Filter_Ptr Filter,int constant){
  Filter->constant=constant;
}

void Print_Filter(Filter_Ptr Filter){

  printf("%d.%d %s %d",
         Filter->rel,
         Filter->col,
         Filter->type,
         Filter->constant);
}
