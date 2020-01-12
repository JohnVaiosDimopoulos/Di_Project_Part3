#ifndef MULTI_JOIN_JOIN_STRUCT_H
#define MULTI_JOIN_JOIN_STRUCT_H

typedef struct Join* Join_Ptr;

Join_Ptr Allocate_Join_Array(int);
void Print_Join(Join_Ptr);
int Is_Self_Join(Join_Ptr);
int Is_Same_Column_used(Join_Ptr,Join_Ptr);
int Is_the_Same_Join(Join_Ptr Join_1,Join_Ptr Join_2);
int Check_if_joins_Connect(Join_Ptr Join_1,Join_Ptr Join_2);


//Join_Struct Accessors//
Join_Ptr Get_Join_by_index(Join_Ptr,int);
int Get_Relation_1(Join_Ptr Join);
int Get_Column_1(Join_Ptr Join);
int Get_Relation_2(Join_Ptr Join);
int Get_Column_2(Join_Ptr Join);

//Join_Struct Mutators//

void Set_Relation_1(int,Join_Ptr);
void Set_Relation_2(int,Join_Ptr);
void Set_Column_1(int,Join_Ptr);
void Set_Column_2(int,Join_Ptr);




#endif //MULTI_JOIN_JOIN_STRUCT_H
