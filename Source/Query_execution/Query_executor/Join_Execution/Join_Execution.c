#include "Join_Execution.h"
#include "../Filter_Executor/Filter_Executor.h"
#include "../../../Basis_Structs/Relation.h"
#include "Relation_Creator/Relation_Creator.h"
#include "Relation_Sorting/Relation_Sorting.h"
#include "Join/Join.h"
#include <stdlib.h>




Intermediate_Result_Ptr Create_Intermediate_Result(){
  Intermediate_Result_Ptr Intermediate_Result = (Intermediate_Result_Ptr)malloc(sizeof(struct Intermediate_Result));
  Intermediate_Result->num_of_results=0;
  for(int i =0;i<4;i++)
    Intermediate_Result->relations_in_result[i]=0;
  Intermediate_Result->row_ids=NULL;
  Intermediate_Result->old_Relation1=NULL;
  Intermediate_Result->old_Relation2=NULL;
  return Intermediate_Result;
}

void Delete_intermediate_Result(Intermediate_Result_Ptr Intermediate_Result){
  Delete_Relation(Intermediate_Result->old_Relation1);
  Delete_Relation(Intermediate_Result->old_Relation2);
  for(int i =0;i<Intermediate_Result->num_of_results;i++){
    free(Intermediate_Result->row_ids[i]);
  }
  free(Intermediate_Result->row_ids);
  free(Intermediate_Result);
}

void Print_Intermediate(Intermediate_Result_Ptr Intermediate_Result){
  for(int i =0;i<Intermediate_Result->num_of_results;i++){
    for(int j =0;j<Intermediate_Result->num_of_relations;j++){
      printf("rel:%d,rowId:%llu|",Intermediate_Result->row_ids[i][j].relation,Intermediate_Result->row_ids[i][j].row_id);
    }
    printf("\n");
  }
}

static Tuple_Ptr* Get_Array_of_data_from_table(Table_Ptr   Relations ,int relation_id,int* num_of_tuples){
  Shell_Ptr Shell_Array = Get_Table_Array(Relations);
  Shell_Ptr Current_Shell = Get_Shell_by_index(Shell_Array,relation_id);
  Tuple_Ptr* Array = Get_Shell_Array(Current_Shell);
  *num_of_tuples = Get_num_of_tuples(Current_Shell);
  return Array;
}

static int find_value_from_row_id(Tuple_Ptr* Array,int col,uint64_t row_id,int num_of_tuples){
  for(int i =0;i<num_of_tuples;i++){
    if(row_id==Array[col][i].row_id)
      return Array[col][i].element;
  }
}

static uint64_t find_row_id(Intermediate_Result_Ptr Intermediate_Result,int relation,int index){

  for(int i=0;i<Intermediate_Result->num_of_relations;i++){
    if(Intermediate_Result->row_ids[index][i].relation==relation)
      return Intermediate_Result->row_ids[index][i].row_id;
  }

}

static RelationPtr Make_Relation_From_Table(Table_Ptr Relations,int rel,int col){
  int num_of_tuples;
  Tuple_Ptr* Array = Get_Array_of_data_from_table(Relations,rel,&num_of_tuples);
  Tuple_Ptr Rel = (Tuple_Ptr)malloc(num_of_tuples* sizeof(struct Tuple));

  for(int i =0;i<num_of_tuples;i++){
    Rel[i].element=Array[col][i].element;
    Rel[i].row_id=Array[col][i].row_id;
  }
  if(num_of_tuples==0)
    return NULL;
  RelationPtr Final_Relation=Create_Relation_with_given_array(num_of_tuples,Rel);
  return Final_Relation;
}

static RelationPtr Make_Relation_From_Intermediate_Array(Table_Ptr Relations,Intermediate_Result_Ptr Intermediate_Result,int rel,int col,int* relations_map){

  int num_of_tuples;

  Tuple_Ptr* Array = Get_Array_of_data_from_table(Relations,relations_map[rel],&num_of_tuples);
  Tuple_Ptr Rel = malloc(Intermediate_Result->num_of_results* sizeof(struct Tuple));

  for(int i =0;i<Intermediate_Result->num_of_results;i++){
    Rel[i].row_id=i;
    uint64_t original_row_id = find_row_id(Intermediate_Result,rel,i);
    int el = Array[col][original_row_id].element;
    Rel[i].element =el;
  }

  RelationPtr Final_Relation=Create_Relation_with_given_array(Intermediate_Result->num_of_results,Rel);
  return Final_Relation;

}

static void Execute_Self_Join(Join_Ptr Join,Table_Ptr Relations) {

  int relation = Get_Relation_1(Join);
  int column_1 = Get_Column_1(Join);
  int column_2 = Get_Column_2(Join);
  Shell_Ptr Relations_Array = Get_Table_Array(Relations);
  Shell_Ptr Relation = Get_Shell_by_index(Relations_Array,relation);
  Tuple_Ptr* Old_Array = Get_Shell_Array(Relation);
  int num_of_tuples=Get_num_of_tuples(Relation);
  int num_of_columns=Get_num_of_columns(Relation);
  int counter=0;


  Tuple_Ptr* Results = (Tuple_Ptr*)malloc((num_of_columns*sizeof(Tuple_Ptr)));
  Results[0] =(Tuple_Ptr)malloc((num_of_columns*num_of_tuples)* sizeof(struct Tuple));

  //setup_pointers;
  Setup_Column_Pointers(Results,num_of_columns,num_of_tuples);

  for(int i=0;i<num_of_tuples;i++){
    if(Old_Array[column_1][i].element==Old_Array[column_2][i].element){
      for(int j =0;j<num_of_columns;j++){
        Results[j][counter].element=Old_Array[j][i].element;
        Results[j][counter].row_id=Old_Array[j][i].row_id;
      }
      counter++;
    }


  }
  Set_Shell_Array(Relation,Results);

  free(Old_Array[0]);
  free(Old_Array);
}

static int Check_if_relations_already_in_result(Join_Ptr Join,Intermediate_Result_Ptr Intermediate_Result){
  if(Intermediate_Result->relations_in_result[Get_Relation_1(Join)]==1 && Intermediate_Result->relations_in_result[Get_Relation_2(Join)]==1)
    return 1;
  return 0;
}

static  Tuple_Ptr  Make_Relation_For_Scan(int relation,int column,Intermediate_Result_Ptr old_result, Table_Ptr Relations,int* relations){

  int num_of_tuples;
  Tuple_Ptr* Array = Get_Array_of_data_from_table(Relations,relations[relation],&num_of_tuples);
  Tuple_Ptr Rel = malloc(old_result->num_of_results*sizeof(struct Tuple));


  for(int i=0;i<old_result->num_of_results;i++){
    Rel[i].row_id=i;
    uint64_t original_row_id = find_row_id(old_result,relation,i);
    int el = Array[column][original_row_id].element;
    Rel[i].element =el;
  }
  return Rel;
}

static void Execute_Scan_Join(Join_Ptr Join,Intermediate_Result_Ptr Intermediate_Result,Table_Ptr Original_Relations,int *relations_map) {

  int rel_1 = Get_Relation_1(Join);
  int col_1 = Get_Column_1(Join);
  Tuple_Ptr Rel_1 = Make_Relation_For_Scan(rel_1, col_1, Intermediate_Result, Original_Relations,relations_map);

  int col_2 = Get_Column_2(Join);
  int rel_2= Get_Relation_2(Join);
  Tuple_Ptr Rel_2 = Make_Relation_For_Scan(rel_2, col_2, Intermediate_Result, Original_Relations,relations_map);


  uint64_t* Res = (uint64_t*)malloc(Intermediate_Result->num_of_results* sizeof(uint64_t));

  int num_of_results=0;

  for(int i=0; i<Intermediate_Result->num_of_results; i++){
    if(Rel_1[i].element==Rel_2[i].element){
      Res[num_of_results]=Rel_1[i].row_id;
      num_of_results++;
    }
  }


  struct Result** result_new = malloc(num_of_results* sizeof(struct Result*));
  for(int i =0;i<num_of_results;i++){
    result_new[i]=malloc(Intermediate_Result->num_of_relations*sizeof(struct Result));
  }


  for(int i =0;i<num_of_results;i++){
    for(int j=0;j<Intermediate_Result->num_of_relations;j++){
      result_new[i][j].row_id=Intermediate_Result->row_ids[Res[i]][j].row_id;
      result_new[i][j].relation=Intermediate_Result->row_ids[Res[i]][j].relation;
    }
  }


  free(Res);
  free(Rel_1);
  free(Rel_2);

  struct Result** temp = Intermediate_Result->row_ids;
  Intermediate_Result->row_ids=result_new;

  for(int i =0; i<Intermediate_Result->num_of_results; i++){
    free(temp[i]);
  }
  free(temp);

  Intermediate_Result->num_of_results=num_of_results;

  Intermediate_Result->old_Relation1=Make_Relation_From_Intermediate_Array(Original_Relations,Intermediate_Result,rel_1,col_1,relations_map);
  Intermediate_Result->old_Relation2=Make_Relation_From_Intermediate_Array(Original_Relations,Intermediate_Result,rel_2,col_2,relations_map);
}

static void Setup_Intermediate_Result(Intermediate_Result_Ptr Intermediate_Result,List_Ptr List,int new_rel){

  struct Result** result = malloc(Get_num_of_results(List)*sizeof(struct Result*));
  for(int i =0;i<Get_num_of_results(List);i++)
    result[i]=malloc((Intermediate_Result->num_of_relations+1)*sizeof(struct Result));

  Node_Ptr temp = Get_head(List);

  int result_counter=0;
  while (temp!=NULL){

    for(int i =0;i<temp->counter;i++){
      int row_in_res = temp->Array[i][1];
      struct Result* temp_res = Intermediate_Result->row_ids[row_in_res];

      for(int j =0;j<Intermediate_Result->num_of_relations;j++){
        result[result_counter][j].row_id=temp_res[j].row_id;
        result[result_counter][j].relation=temp_res[j].relation;
      }

      result[result_counter][Intermediate_Result->num_of_relations].relation=new_rel;
      result[result_counter][Intermediate_Result->num_of_relations].row_id=temp->Array[i][0];
      result_counter++;
    }
    temp=temp->next;

  }

  Delete_List(List);

  struct Result** des = Intermediate_Result->row_ids;
  Intermediate_Result->row_ids=result;

  for(int i =0;i<Intermediate_Result->num_of_results;i++){
    free(des[i]);
  }
  free(des);

  Intermediate_Result->num_of_results=Get_num_of_results(List);
  Intermediate_Result->relations_in_result[new_rel]=1;
  Intermediate_Result->num_of_relations++;
}

static void Execute_Normal_Join(Join_Ptr Current_Join,Intermediate_Result_Ptr Intermediate_Result,Table_Ptr Relations,Table_Ptr Original_Relations,int *relations_map) {

  int rel_1 = Get_Relation_1(Current_Join);
  int col_1 = Get_Column_1(Current_Join);
  int rel_2 = Get_Relation_2(Current_Join);
  int col_2 = Get_Column_2(Current_Join);

  RelationPtr Final_Relation_1,Final_Relation_2;
  List_Ptr List;

  if(Intermediate_Result->relations_in_result[Get_Relation_1(Current_Join)]==1){

    Final_Relation_1 = Make_Relation_From_Table(Relations,rel_2,col_2);
    Final_Relation_2 = Make_Relation_From_Intermediate_Array(Original_Relations,Intermediate_Result,rel_1,col_1,relations_map);
//    Print_Relation(Final_Relation_2);

    if(Final_Relation_1==NULL || Final_Relation_2==NULL){
      Intermediate_Result->row_ids=NULL;
      return;
    }


    Sort(Final_Relation_1);
    Sort(Final_Relation_2);

    List=Execute_Join(Final_Relation_1,Final_Relation_2);
    Delete_Relation(Final_Relation_1);
    Delete_Relation(Final_Relation_2);
    Setup_Intermediate_Result(Intermediate_Result,List,rel_2);

    Intermediate_Result->old_Relation1=Make_Relation_From_Intermediate_Array(Original_Relations,Intermediate_Result,rel_1,col_1,relations_map);
    Intermediate_Result->old_Relation2=Make_Relation_From_Intermediate_Array(Original_Relations,Intermediate_Result,rel_2,col_2,relations_map);

  }



  else if(Intermediate_Result->relations_in_result[Get_Relation_2(Current_Join)]==1){
    Final_Relation_1 = Make_Relation_From_Table(Relations,rel_1,col_1);
    Final_Relation_2 = Make_Relation_From_Intermediate_Array(Original_Relations,Intermediate_Result,rel_2,col_2,relations_map);

    if(Final_Relation_1==NULL || Final_Relation_2==NULL){
      Intermediate_Result->row_ids=NULL;
      return;
    }


    List = Execute_Join(Final_Relation_1,Final_Relation_2);
    Delete_Relation(Final_Relation_1);
    Delete_Relation(Final_Relation_2);
    Setup_Intermediate_Result(Intermediate_Result,List,rel_1);
    Intermediate_Result->old_Relation1=Make_Relation_From_Intermediate_Array(Original_Relations,Intermediate_Result,rel_1,col_1,relations_map);
    Intermediate_Result->old_Relation2=Make_Relation_From_Intermediate_Array(Original_Relations,Intermediate_Result,rel_2,col_2,relations_map);


  }



  else{
    Final_Relation_1= Make_Relation_From_Table(Relations,rel_1,col_1);
    Final_Relation_2= Make_Relation_From_Table(Relations,rel_2,col_2);

    if(Final_Relation_1==NULL || Final_Relation_2==NULL){
      Intermediate_Result->row_ids=NULL;
      return;
    }


    Sort(Final_Relation_1);
    Sort(Final_Relation_2);

    List_Ptr List = Execute_Join(Final_Relation_1,Final_Relation_2);
    Delete_Relation(Final_Relation_1);
    Delete_Relation(Final_Relation_2);

    //initalize intermediate
    if(Get_num_of_results(List)==0){
      Intermediate_Result->row_ids=NULL;
      return;
    }

    struct Result** result = malloc(Get_num_of_results(List)*sizeof(struct Result*));
    for(int i =0;i<Get_num_of_results(List);i++)
      result[i]=malloc(2*sizeof(struct Result));
    Node_Ptr temp = Get_head(List);


    int result_counter=0;
    while (temp!=NULL){
      for(int i =0;i<temp->counter;i++){

        result[result_counter][0].row_id=temp->Array[i][0];
        result[result_counter][0].relation=rel_1;
        result[result_counter][1].row_id=temp->Array[i][1];
        result[result_counter][1].relation=rel_2;
        result_counter++;

      }
      temp=temp->next;
    }

    Delete_List(List);

    Intermediate_Result->row_ids=result;
    Intermediate_Result->num_of_relations=2;
    Intermediate_Result->num_of_results=Get_num_of_results(List);
    Intermediate_Result->relations_in_result[rel_1]=1;
    Intermediate_Result->relations_in_result[rel_2]=1;


    Intermediate_Result->old_Relation1=Make_Relation_From_Intermediate_Array(Original_Relations,Intermediate_Result,rel_1,col_1,relations_map);
    Intermediate_Result->old_Relation2=Make_Relation_From_Intermediate_Array(Original_Relations,Intermediate_Result,rel_2,col_2,relations_map);
  }


}

static void Execute_Same_Column_Join(Join_Ptr Current_Join,Join_Ptr Last_Join,Intermediate_Result_Ptr Intermediate_Result,Table_Ptr Relations,Table_Ptr Original_Relations,int *relations_map){

  int rel_1 = Get_Relation_1(Current_Join);
  int col_1 = Get_Column_1(Current_Join);
  int rel_2 = Get_Relation_2(Current_Join);
  int col_2 = Get_Column_2(Current_Join);

  RelationPtr Final_Relation_1,Final_Relation_2;
  List_Ptr List;

  if(Intermediate_Result->relations_in_result[rel_1]==1){
    if(rel_1==Get_Relation_1(Last_Join)&&col_1==Get_Column_1(Last_Join)){
      Final_Relation_2=Intermediate_Result->old_Relation1;
    }
    else{
      Final_Relation_2=Intermediate_Result->old_Relation2;
    }
    Final_Relation_1=Make_Relation_From_Table(Relations,rel_2,col_2);
    if(Final_Relation_1==NULL || Final_Relation_2==NULL){
      Intermediate_Result->row_ids=NULL;
      return;
    }

    Sort(Final_Relation_1);

    List = Execute_Join(Final_Relation_1,Final_Relation_2);
    Delete_Relation(Final_Relation_1);
    Delete_Relation(Final_Relation_2);
    Setup_Intermediate_Result(Intermediate_Result,List,rel_2);
    Intermediate_Result->old_Relation1=Make_Relation_From_Intermediate_Array(Original_Relations,Intermediate_Result,rel_1,col_1,relations_map);
    Intermediate_Result->old_Relation2=Make_Relation_From_Intermediate_Array(Original_Relations,Intermediate_Result,rel_2,col_2,relations_map);

  }
  else if (Intermediate_Result->relations_in_result[rel_2]==1){
    Final_Relation_1=  Make_Relation_From_Table(Relations,rel_1,col_1);
    if(rel_2==Get_Relation_1(Last_Join)&& col_2==Get_Column_1(Last_Join))
      Final_Relation_2=Intermediate_Result->old_Relation1;
    else
      Final_Relation_2=Intermediate_Result->old_Relation2;

    if(Final_Relation_1==NULL || Final_Relation_2==NULL){
      Intermediate_Result->row_ids=NULL;
      return;
    }

    Sort(Final_Relation_1);

    List = Execute_Join(Final_Relation_1,Final_Relation_2);
    Delete_Relation(Final_Relation_1);
    Delete_Relation(Final_Relation_2);
    Setup_Intermediate_Result(Intermediate_Result,List,rel_1);
    Intermediate_Result->old_Relation1=Make_Relation_From_Intermediate_Array(Original_Relations,Intermediate_Result,rel_1,col_1,relations_map);
    Intermediate_Result->old_Relation2=Make_Relation_From_Intermediate_Array(Original_Relations,Intermediate_Result,rel_2,col_2,relations_map);

  }

}

Intermediate_Result_Ptr Execute_Joins(Execution_Queue_Ptr Execution_Queue, Table_Ptr Filtered_Relations,Table_Ptr Original_Relations,int* relation_map){

  Intermediate_Result_Ptr Intermediate_Result = Create_Intermediate_Result();
  Join_Ptr Last_Join = NULL;


  for(Join_Ptr Current_Join = Pop_Next_join(Execution_Queue);
      Current_Join!=NULL;
      Current_Join=Pop_Next_join(Execution_Queue)){

    if(Is_Self_Join(Current_Join))
      Execute_Self_Join(Current_Join, Filtered_Relations);

    else if(Check_if_relations_already_in_result(Current_Join,Intermediate_Result))
      Execute_Scan_Join(Current_Join, Intermediate_Result, Original_Relations,relation_map);
    else if (Is_Same_Column_used(Current_Join,Last_Join)){
      Execute_Same_Column_Join(Current_Join,Last_Join,Intermediate_Result,Filtered_Relations,Original_Relations,relation_map);
    }
    else{
      Execute_Normal_Join(Current_Join,Intermediate_Result,Filtered_Relations,Original_Relations,relation_map);
      if(Intermediate_Result->row_ids==NULL)
        return  NULL;
    }

    Last_Join=Current_Join;

  }
  return Intermediate_Result;

}
