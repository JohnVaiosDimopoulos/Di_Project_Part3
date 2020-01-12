#include "Join_Struct.h"
#include <stdlib.h>
#include <stdio.h>

struct Join{
  int rel1;
  int col1;
  int rel2;
  int col2;
};

void Print_Join(Join_Ptr Join){
  printf("%d.%d = %d.%d",
         Join->rel1,
         Join->col1,
         Join->rel2,
         Join->col2);
}

Join_Ptr Allocate_Join_Array(int num_of_joins){
  Join_Ptr Join_Array= (Join_Ptr)malloc(num_of_joins * sizeof(struct Join));
  return Join_Array;
}

int Is_Self_Join(Join_Ptr Join){
  if(Join->rel1==Join->rel2)
    return 1;
  return 0;
}

int Is_Same_Column_used(Join_Ptr Join_1,Join_Ptr Join_2){
  if(Join_1==NULL|| Join_2==NULL)
    return 0;
  if( (Join_1->rel1==Join_2->rel1 && Join_1->col1==Join_2->col1)||
      (Join_1->rel1==Join_2->rel2 && Join_1->col1==Join_2->col2 )||
      (Join_1->rel2==Join_2->rel1 && Join_1->col2==Join_2->col1)||
      (Join_1->rel2==Join_2->rel2 && Join_1->col2==Join_2->col2))
    return 1;
  return 0;
}

int Is_the_Same_Join(Join_Ptr Join_1,Join_Ptr Join_2){
  if(Join_1->rel1 == Join_2->rel2 &&
     Join_1->col1==Join_2->col2 &&
     Join_1->rel2== Join_2->rel1 &&
     Join_1->col2 == Join_2->col1)
    return 1;

  else if(
      Join_1->rel1==Join_2->rel1 &&
      Join_1->col1==Join_2->col1 &&
      Join_1->rel2==Join_2->rel2 &&
      Join_1->col2==Join_2->col2)
    return 1;
  return 0;
}

int Check_if_joins_Connect(Join_Ptr Join_1,Join_Ptr Join_2){
  if(Join_1->rel1==Join_2->rel1 || Join_1->rel1==Join_2->rel2 || Join_1->rel2==Join_2->rel1 || Join_1->rel2==Join_2->rel2)
    return 1;
  return 0;

}

Join_Ptr Get_Join_by_index(Join_Ptr Join_array,int index){
  return &(Join_array[index]);
}

int Get_Relation_1(Join_Ptr Join){
  return Join->rel1;
}

int Get_Column_1(Join_Ptr Join){
  return Join->col1;
}

int Get_Relation_2(Join_Ptr Join){
  return Join->rel2;
}

int Get_Column_2(Join_Ptr Join){
  return Join->col2;
}

void Set_Relation_1(int rel1,Join_Ptr Join){
  Join->rel1=rel1;
}

void Set_Relation_2(int rel2,Join_Ptr Join){
  Join->rel2=rel2;
}

void Set_Column_1(int col1,Join_Ptr Join){
  Join->col1=col1;
}

void Set_Column_2(int col2,Join_Ptr Join){
  Join->col2=col2;
}
