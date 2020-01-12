#include <stdlib.h>
#include "Histogram.h"

struct Histogram{
  int Array[HISTOGRAM_SIZE];
};

static void Fill_Histogram(RelationPtr Relation, Histogram_Ptr Histogram, const int byte_to_check) {

 uint8_t current_byte;
 for(int i =0;i<Relation->num_of_tuples;i++){
   current_byte = Relation->tuples[i].element>>((byte_to_check-1)*8) & 0xff;
   Histogram->Array[current_byte]++;
 }
}

static Histogram_Ptr Create_And_Init_Histogram() {

  Histogram_Ptr Histogram = (Histogram_Ptr)malloc(sizeof(struct Histogram));
  for(int i =0;i<HISTOGRAM_SIZE;i++)
    Histogram->Array[i]=0;
  return Histogram;
}



Histogram_Ptr Get_Histogram(RelationPtr Relation, const int byte_to_check) {

  if(byte_to_check>8){
    printf("byte_to_check out of range");
    exit(-1);
  }

  Histogram_Ptr Histogram = Create_And_Init_Histogram();
  Fill_Histogram(Relation,Histogram,byte_to_check);
  return Histogram;
}

void Print_Histogram(Histogram_Ptr Histogram){
  for(int i =0;i<HISTOGRAM_SIZE;i++){
    if(Histogram->Array[i]==0)
      continue;
    printf("value:%d   quantity:%d\n",i,Histogram->Array[i]);
  }

  printf("\n\n");
}


int* Get_Histogram_Array(Histogram_Ptr Histogram){
  return Histogram->Array;
}

void Delete_Histogram(Histogram_Ptr Histogram){
  free(Histogram);
}

