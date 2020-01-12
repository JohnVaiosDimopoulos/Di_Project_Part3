#include "Argument_Manager.h"
#include "../../Util/Utilities.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define EXPECTED_ARG_COUNT 7
#define DIR_FLAG "-d"
#define INIT_FLAG "-i"
#define WORK_FLAG "-w"

struct Arg_Manager {
  int argc;
  char** argv;
};

static int Check_Arguments_Number(int argc) {
  if(argc!=EXPECTED_ARG_COUNT){
    printf("%d",argc);
    printf("%s","Wrong Number of Arguments\n");
    return 0;
  }
  return 1;
}

/*
 * Gets through the arguments and stores the filenames into File_Name_1 and File_Name_2
 * Expected Format -i .init file -w .work file -d direcotry path
 * On success returns 1
 * On failure returns 0
 * */
static int Go_Through_Argv_And_Get_FileNames(Arg_Manager_Ptr Manager,
                                             char **Init_FileName,
                                             char **Work_FileName,
                                             char **Dir_Name) {

  for(int i=1;i<Manager->argc;i++){
    if(!strcmp(INIT_FLAG, Manager->argv[i])){
      i++;
      *Init_FileName = Allocate_and_Copy_Str(Manager->argv[i]);
    }
    else if(!strcmp(WORK_FLAG, Manager->argv[i])){
      i++;
      *Work_FileName= Allocate_and_Copy_Str(Manager->argv[i]);
    }
    else if(!strcmp(DIR_FLAG, Manager->argv[i])){
      i++;
      *Dir_Name= Allocate_and_Copy_Str(Manager->argv[i]);
    }
    else{
      printf("%s","Wrong Argument format\n");
      return 0;
    }
  }
  return 1;
}


/* On success returns a Table_FileNames pointer with the names of the two files inside
 * On Failure returns NULL
 */
Argument_Data_Ptr Get_Argument_Data(Arg_Manager_Ptr Manager){
  char* Init_FileName=NULL;
  char* Work_FileName=NULL;
  char* Dir_Name = NULL;

  if(!Check_Arguments_Number(Manager->argc)){
    exit(-1);
  }


  if(!Go_Through_Argv_And_Get_FileNames(Manager, &Init_FileName, &Work_FileName, &Dir_Name))
    exit(-1);

  if(Init_FileName!=NULL && Work_FileName!=NULL ){
    Argument_Data_Ptr Argument_Data = Create_Argument_Data(Init_FileName, Work_FileName, Dir_Name);
    free(Init_FileName);
    free(Work_FileName);
    free(Dir_Name);
    return Argument_Data;
  }


  return NULL;
}


Arg_Manager_Ptr Create_ArgManager(int argc, char** argv){
 Arg_Manager_Ptr Manager =(Arg_Manager_Ptr)malloc(sizeof(struct Arg_Manager));
 Manager->argv=argv;
 Manager->argc=argc;
 return Manager;
}

void Delete_ArgManager(Arg_Manager_Ptr Manager){
  Manager->argv=NULL;
  free(Manager);
}
