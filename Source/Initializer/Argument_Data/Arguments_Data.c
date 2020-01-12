#include "Arguments_Data.h"
#include <stdlib.h>
#include "../../Util/Utilities.h"

struct Arguments_Data{
  char* Init_FileName;
  char* Work_FileName;
  char* Dir_Name;

};

Argument_Data_Ptr Create_Argument_Data(const char *Init_FileName, const char *Work_Filename, const char *Dir_Name) {
  Argument_Data_Ptr Data = (Argument_Data_Ptr)malloc(sizeof(struct Arguments_Data));
  Data->Init_FileName= Allocate_and_Copy_Str(Init_FileName);
  Data->Work_FileName= Allocate_and_Copy_Str(Work_Filename);
  Data->Dir_Name=Allocate_and_Copy_Str(Dir_Name);
  return Data;
}

void Delete_Argument_Data(Argument_Data_Ptr Data){
  free(Data->Init_FileName);
  free(Data->Work_FileName);
  free(Data->Dir_Name);
  free(Data);
}

const char* Get_Init_FileName(Argument_Data_Ptr Data){
  return Data->Init_FileName;
}

const char* Get_Work_FileName(Argument_Data_Ptr Data){
  return Data->Work_FileName;
}

const char* Get_Dir_Name(Argument_Data_Ptr Data){
  return Data->Dir_Name;
}


