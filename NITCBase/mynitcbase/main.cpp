#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include <iostream>

void printAttributeCat(){
    
  // create objects for the relation catalog and attribute catalog
  RecBuffer relCatBuffer(RELCAT_BLOCK);
  RecBuffer attrCatBuffer(ATTRCAT_BLOCK);

  HeadInfo relCatHeader;
  HeadInfo attrCatHeader;

  // load the headers of both the blocks into relCatHeader and attrCatHeader.
  // (we will implement these functions later)
  relCatBuffer.getHeader(&relCatHeader);
  attrCatBuffer.getHeader(&attrCatHeader);

  for (int i = 0 ; i < relCatHeader.numEntries ; i++) {

    Attribute relCatRecord[RELCAT_NO_ATTRS]; // will store the record from the relation catalog

    relCatBuffer.getRecord(relCatRecord, i);

    printf("Relation: %s\n", relCatRecord[RELCAT_REL_NAME_INDEX].sVal);

    for (int j = 0 ; j<attrCatHeader.numEntries ; j++) {

      // declare attrCatRecord and load the attribute catalog entry into it
      Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

      attrCatBuffer.getRecord(attrCatRecord, j);

      if (strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal,relCatRecord[RELCAT_REL_NAME_INDEX].sVal) == 0) {
        const char *attrType = attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER ? "NUM" : "STR";
        printf("  %s: %s\n", attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrType);
      }
    }
    printf("\n");
  }
}

void updateAttrbuteCat(const char* relName,const char* oldAttrName,const char* newAttrName){
  
  RecBuffer attrCatBuffer(ATTRCAT_BLOCK);
  HeadInfo attrCatHeader;
  attrCatBuffer.getHeader(&attrCatHeader);

  for(int recIndex = 0 ; recIndex < attrCatHeader.numEntries ; recIndex++){
    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    attrCatBuffer.getRecord(attrCatRecord,recIndex);

    if(strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal,relName) == 0 and strcmp(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,oldAttrName) == 0){
      strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,newAttrName);
      attrCatBuffer.setRecord(attrCatRecord,recIndex);
      break;
    }

    if(recIndex == attrCatHeader.numSlots-1){
      recIndex = -1;
      attrCatBuffer = RecBuffer (attrCatHeader.rblock);
      attrCatBuffer.getHeader(&attrCatHeader);
    }
  }
  
}

int main(int argc, char *argv[]) {
  Disk disk_run;
  StaticBuffer buffer;
  OpenRelTable cache;


  return FrontendInterface::handleFrontend(argc, argv);

  /*unsigned char buffer[BLOCK_SIZE];
  Disk::readBlock(buffer, 7000);
  char message[] = "hello";
  memcpy(buffer + 20, message, 6);
  Disk::writeBlock(buffer, 7000);

  unsigned char buffer2[BLOCK_SIZE];
  char message2[6];
  Disk::readBlock(buffer2, 7000);
  memcpy(message2, buffer2 + 20, 6);
  std::cout << message2;

  
  //updateAttrbuteCat("Students","Class","Branch");
  //printAttributeCat();

  RelCatEntry relCatBuf;
  AttrCatEntry attrCatBuf;

  for(int relId = 0 ; relId < 3 ; relId++){
    RelCacheTable::getRelCatEntry(relId,&relCatBuf);
    printf("Relation: %s\n",relCatBuf.relName);
    for(int attrIndex = 0 ; attrIndex<relCatBuf.numAttrs ; attrIndex++){
      AttrCacheTable::getAttrCatEntry(relId, attrIndex, &attrCatBuf);
      const char* attrType = attrCatBuf.attrType == NUMBER ? "NUM" : "STR" ;
      printf("%s: %s\n",attrCatBuf.attrName,attrType);
    }
    printf("\n");
  }
  */

}