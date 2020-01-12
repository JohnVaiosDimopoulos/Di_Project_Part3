#ifndef MULTI_JOIN_FILTER_H
#define MULTI_JOIN_FILTER_H
typedef struct Filter* Filter_Ptr;

Filter_Ptr Allocate_filter_Array(int);
Filter_Ptr Get_Filter_by_index(Filter_Ptr,int);
//Filter Accessors//
int Get_Filter_Relation(Filter_Ptr Filter);
int Get_Filter_Column(Filter_Ptr Filter);
int Get_Constant(Filter_Ptr Filter);
char* Get_Type(Filter_Ptr Filter);

//Filter_Mutators//
void Set_Relation(Filter_Ptr Filter_array,int rel);
void Set_Column(Filter_Ptr Filter_array,int col);
void Set_type(Filter_Ptr Filter_array,char type[2]);
void Set_constant(Filter_Ptr Filter_array,int constant);

void Print_Filter(Filter_Ptr Filter);

#endif //MULTI_JOIN_FILTER_H
