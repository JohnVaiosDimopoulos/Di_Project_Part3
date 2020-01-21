#include "Table_Allocator.h"
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <stdbool.h> 

#include "../../Util/Utilities.h"

#define N 50000000

struct Table_Allocator{
  char* Init_Filename;
  char* Dir_Name;
};


struct Table {
  Shell_Ptr Array;
  int num_of_shells;
};

struct Shell {
  uint64_t num_of_tuples;
  uint64_t num_of_columns;
  Tuple_Ptr* Array;
  Column_Stats_Ptr stats;
  bool **d_array;
};

struct Column_Stats {
  uint64_t l;
  uint64_t u;
  int64_t f;
  int64_t d;
};

Table_AllocatorPtr Create_Table_Allocator(Argument_Data_Ptr Data){
  Table_AllocatorPtr Table_Allocator = (Table_AllocatorPtr)malloc(sizeof(Table_Allocator));
  Table_Allocator->Dir_Name=Allocate_and_Copy_Str(Get_Dir_Name(Data));
  Table_Allocator->Init_Filename=Allocate_and_Copy_Str(Get_Init_FileName(Data));
  return Table_Allocator;
}

void Delete_Table_Allocator(Table_AllocatorPtr Table_Allocator){
  free(Table_Allocator->Init_Filename);
  free(Table_Allocator->Dir_Name);
  free(Table_Allocator);
}

static int Count_Shells(Table_AllocatorPtr Table_Allocator){
  const char* Init_File = construct_Path(Table_Allocator->Init_Filename,Table_Allocator->Dir_Name);
  FILE* FilePtr;
  Open_File_for_Read(&FilePtr,Init_File);
  int file_lines = Count_File_Lines(FilePtr);
  free(Init_File);
  return file_lines;
}

Table_Ptr Allocate_Table(Table_AllocatorPtr Table_Allocator){
  int num_of_tables = Count_Shells(Table_Allocator);
  Table_Ptr Table = (Table_Ptr)malloc(sizeof(Table));
  Table->Array = malloc(num_of_tables * sizeof(struct Shell));
  Table->num_of_shells = num_of_tables;
  return Table;
}

Shell_Ptr Get_Table_Array(Table_Ptr Table) {
	return Table->Array;
}

int Get_num_of_shells(Table_Ptr Table) {
	return Table->num_of_shells;
}

static char* Get_File_Name(char* line_buffer, int size) {
  char* file_Name = malloc(sizeof(char)*size);
  sscanf(line_buffer, "%s\n", file_Name);
  return file_Name;
}

//void Print_Relation(Tuple_Ptr* Relation,int num_of_tuples,int num_of_columns){
//  for(int i =0;i<num_of_tuples;i++){
//    for(int j=0;j<num_of_columns;j++){
//      printf("(%llu)",Relation[j][i].row_id);
//      printf("%llu",Relation[j][i].element);
//    }
//    printf("\n");
//  }
//}


void Print_Shell(Shell_Ptr Shell, FILE *fp) {
  fprintf(fp, "sizes: %llu %llu\n", Shell->num_of_tuples, Shell->num_of_columns);
  for(int i = 0; i < Shell->num_of_columns; i++)
    fprintf(fp, "stats: l = %llu u = %llu f = %lu d = %lu\n", Shell->stats[i].l, Shell->stats[i].u, Shell->stats[i].f, Shell->stats[i].d);
  fprintf(fp, "\n");
  for(int i =0;i<Shell->num_of_tuples;i++){
    for(int j =0;j<Shell->num_of_columns;j++) {
      fprintf(fp,"(%llu)",Shell->Array[j][i].row_id);
      fprintf(fp, "%llu|", Shell->Array[j][i].element);
	}
    fprintf(fp,"\n");
  }
}

void Print_Table(Table_Ptr Table) {
  FILE *fp;
  Open_File_for_Write(&fp, "element.txt");
  for(int i = 0; i < Table->num_of_shells; i++){
    fprintf(fp,"=====REL %d=====\n",i);
    Print_Shell(&Table->Array[i], fp);
    fprintf(fp,"================\n\n\n");
  }
}

void Allocate_Shell(Shell_Ptr Shell){
  Shell->Array = malloc(Shell->num_of_columns * sizeof(Tuple_Ptr));
  Shell->Array[0]=malloc((Shell->num_of_columns*Shell->num_of_tuples)* sizeof(struct Tuple));
  
  Shell->stats = (Column_Stats_Ptr)malloc(Shell->num_of_columns * sizeof(struct Column_Stats));
  Shell->d_array = (bool**)malloc(Shell->num_of_columns * sizeof(bool* ));
}

static void Read_from_File(uint64_t* data ,FILE* fp){

  if(fread(data, sizeof(uint64_t), 1, fp) < 0) {
    printf("error in open\n"); exit(1);
  }
}

static int Element_Exists(Tuple_Ptr Tuple, uint64_t el, uint64_t tuples) {
  //printf("element %llu (%llu tuples so far)\n", el, tuples);
  for(int i = 0; i < tuples; i++){
   // printf("\t%llu \n", Tuple[i]);
    if(Tuple[i].element == el) {
      //printf("element %llu (%llu tuples so far)\n", el, tuples);
      //printf("exists\n");
      return 1;
    }
  }
  //printf("does not exist\n");
  return 0;
}

static void Read_Data(Shell_Ptr Shell,FILE* fp){
  for(int i =0;i<Shell->num_of_columns;i++){
    Shell->stats[i].l = INT_MAX;
    Shell->stats[i].u = 0;
    Shell->stats[i].d = 0;

    for(int j=0;j<Shell->num_of_tuples;j++){
      Read_from_File(&Shell->Array[i][j].element, fp);
      Shell->Array[i][j].row_id=j;
	  
	  //find min and max of the column
	  if(Shell->Array[i][j].element < Shell->stats[i].l)
	    Shell->stats[i].l = Shell->Array[i][j].element;
	  if(Shell->Array[i][j].element > Shell->stats[i].u)
	    Shell->stats[i].u = Shell->Array[i][j].element;
	  
	  //if(!Element_Exists(Shell->Array[i], Shell->Array[i][j].element, j))
	  //  Shell->stats[i].d++;
    }
	Shell->stats[i].f = Shell->num_of_tuples;

	uint64_t s = Shell->stats[i].u - Shell->stats[i].l + 1;
	uint64_t diff = Shell->stats[i].l;
	if(s > N) {
      printf("NEVER\n");
      s = N;
      diff = Shell->stats[i].l % N;
	}

	Shell->d_array[i] = (bool*)malloc(s * sizeof(bool));
    for(int j = 0; j < Shell->num_of_tuples; j++){
      if(Shell->Array[i][j].element - diff > 0 && 
		  Shell->Array[i][j].element - diff < s && 
		  Shell->d_array[i][Shell->Array[i][j].element - diff] != true) {
        Shell->d_array[i][Shell->Array[i][j].element - diff] = true;
	    Shell->stats[i].d++;
	  }
    }

//	for(int j = 0; j < s; j++) {
//		if(Shell->d_array[i][j] == false) printf("FALSE\n");
//		if(Shell->d_array[i][j] == true) printf("TRUE\n");
//	}

  }
}

void Setup_Column_Pointers(Tuple_Ptr* Array,int num_of_columns,int num_of_tuples){
  int last_index = 0;
  for(int i =1;i<num_of_columns;i++){
    Array[i]=&Array[0][last_index+num_of_tuples];
    last_index+=num_of_tuples;
  }
}


static void Fill_Shell(const char* FileName, Shell_Ptr Shell){

  //1. open file
  FILE* fp = fopen (FileName, "rb");
  //2. read num_of_tuples and num_of_columns
  Read_from_File(&Shell->num_of_tuples,fp);
  Read_from_File(&Shell->num_of_columns,fp);
  //3. Allocate Space
  Allocate_Shell(Shell);
  //set up column pointers
  Setup_Column_Pointers(Shell->Array,Shell->num_of_columns,Shell->num_of_tuples);
  //4.Read Data
  Read_Data(Shell,fp);
  fclose(fp);
}

Table_Ptr Make_Table_For_Joins(Table_Ptr Relations, int* relations,int num_of_relations){

  Table_Ptr New_Table = (Table_Ptr)malloc(sizeof(struct Table));
  New_Table->Array=(Shell_Ptr)malloc(num_of_relations* sizeof(struct Shell));
  New_Table->num_of_shells=num_of_relations;

  for(int i=0;i<num_of_relations;i++){
    int Original_Shell_index = relations[i];
    int num_of_tuples=Relations->Array[Original_Shell_index].num_of_tuples;
    int num_of_columns=Relations->Array[Original_Shell_index].num_of_columns;
    New_Table->Array[i].num_of_tuples=num_of_tuples;
    New_Table->Array[i].num_of_columns=num_of_columns;
    New_Table->Array[i].Array=malloc(num_of_columns* sizeof(Tuple_Ptr));
    New_Table->Array[i].Array[0]=malloc((num_of_columns*num_of_tuples)* sizeof( struct Tuple));
    Setup_Column_Pointers(New_Table->Array[i].Array,num_of_columns,num_of_tuples);
	//Copy data and row ids
    for(int j =0;j<num_of_columns;j++){
      for(int k=0;k<num_of_tuples;k++){
        New_Table->Array[i].Array[j][k].element = Relations->Array[Original_Shell_index].Array[j][k].element;
        New_Table->Array[i].Array[j][k].row_id = Relations->Array[Original_Shell_index].Array[j][k].row_id;
      }

    }
	//Copy stats
	New_Table->Array[i].stats = (Column_Stats_Ptr)malloc(New_Table->Array[i].num_of_columns * sizeof(struct Column_Stats));
	New_Table->Array[i].d_array = (bool**)malloc(New_Table->Array[i].num_of_columns * sizeof(bool*));
	for(int j = 0; j < num_of_columns; j++) {
      New_Table->Array[i].stats[j].l = Relations->Array[Original_Shell_index].stats[j].l;
      New_Table->Array[i].stats[j].u = Relations->Array[Original_Shell_index].stats[j].u;
      New_Table->Array[i].stats[j].f = Relations->Array[Original_Shell_index].stats[j].f;
      New_Table->Array[i].stats[j].d = Relations->Array[Original_Shell_index].stats[j].d;

	  uint64_t s = New_Table->Array[i].stats[j].u - New_Table->Array[i].stats[j].l + 1;
	  //printf("sizeof = %lu\n", s);
	  New_Table->Array[i].d_array[j] = (bool*)malloc(s * sizeof(bool));
	  for(int k = 0; k < s; k++) {
        New_Table->Array[i].d_array[j][k] = Relations->Array[Original_Shell_index].d_array[j][k];
      }
	}
  }

  return New_Table;

}

void Fill_Table(Table_Ptr Table, Table_AllocatorPtr Table_Allocator) {

  FILE *Init_File;
  char *line_buffer = NULL;
  size_t line_buffer_size = 0;

  //1.construct initFilePath
  const char *Init_File_Path = construct_Path(Table_Allocator->Init_Filename, Table_Allocator->Dir_Name);
  //2.open init file
  Open_File_for_Read(&Init_File, Init_File_Path);

  //3.read line by line
  for (int i = 0; i < Table->num_of_shells; i++) {
    int read = getline(&line_buffer, &line_buffer_size, Init_File);
    char* File_Name = Get_File_Name(line_buffer,read);
    const char *file_Path = construct_Path(File_Name, Table_Allocator->Dir_Name);
    Fill_Shell(file_Path, &Table->Array[i]);
    free(file_Path);
    free(File_Name);
  }
  //fclose(Init_File);
  free(line_buffer);
  free(Init_File_Path);
}

static void Delete_Shell(struct Shell Shell) {
  free(Shell.Array[0]);
  free(Shell.Array);
  free(Shell.stats);
  for(int i = 0; i < Shell.num_of_columns; i++)
    free(Shell.d_array[i]);
  free(Shell.d_array);
}

void Delete_Table(Table_Ptr Table) {
  for(int i =0;i<Table->num_of_shells;i++)
    Delete_Shell(Table->Array[i]);
  free(Table->Array);
  free(Table);
}

///////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////

void Set_Column_l(Shell_Ptr Shell, uint64_t i, uint64_t l){
  Shell->stats[i].l = l;
}
void Set_Column_u(Shell_Ptr Shell, uint64_t i, uint64_t u){
  Shell->stats[i].u = u;
}
void Set_Column_f(Shell_Ptr Shell, uint64_t i, int64_t f){
  Shell->stats[i].f = f;
}
void Set_Column_d(Shell_Ptr Shell, uint64_t i, int64_t d){
  Shell->stats[i].d = d;
}

void Set_Shell_Array(Shell_Ptr Shell, Tuple_Ptr *Array){
  Shell->Array = Array;
}

void Set_Shell_num_of_tuples(Shell_Ptr Shell, int tuples){
  Shell->num_of_tuples = tuples;
}

uint64_t Get_num_of_tuples(Shell_Ptr Shell) {
  return Shell->num_of_tuples;
}
uint64_t Get_num_of_columns(Shell_Ptr Shell) {
  return Shell->num_of_columns;
}

Shell_Ptr Get_Shell_by_index(Shell_Ptr Shell,int index){
  return &Shell[index];
}

Tuple_Ptr* Get_Shell_Array(Shell_Ptr Shell){
  return Shell->Array;
}

Tuple_Ptr Get_Shell_Array_by_index(Shell_Ptr Shell, int i, int j){
  return &Shell->Array[i][j];
}

uint64_t Get_Data(Tuple_Ptr Tuple){
  return Tuple->element;
}

uint64_t Get_Row_id(Tuple_Ptr Tuple){
  return Tuple->row_id;
}

Tuple_Ptr Get_Column(Shell_Ptr Shell,int column_id){
  return Shell->Array[column_id];
}

uint64_t Get_Shell_stats(Shell_Ptr Shell){
  return Shell->stats;
}

uint64_t Get_Column_l(Shell_Ptr Shell, uint64_t i){
  //printf("from getter %llu %llu\n", Shell->num_of_tuples, Shell->num_of_columns);
  return Shell->stats[i].l;
}

uint64_t Get_Column_u(Shell_Ptr Shell, uint64_t i){
  return Shell->stats[i].u;
}

int64_t Get_Column_f(Shell_Ptr Shell, uint64_t i){
  return Shell->stats[i].f;
}

int64_t Get_Column_d(Shell_Ptr Shell, uint64_t i){
  return Shell->stats[i].d;
}


bool** Get_d_array(Shell_Ptr Shell){
  return Shell->d_array;
}

//Table_Ptr Allocate_Table_with_num_of_Shells(int num_of_shells) {
//  Table_Ptr Table = (Table_Ptr)malloc(sizeof(Table));
//  Table->Array = malloc(num_of_shells * sizeof(struct Shell));
//  Table->num_of_shells = num_of_shells;
//  return Table;
//}
