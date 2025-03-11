#include "BlockBuffer.h"

#include <cstdlib>
#include <cstring>

// the declarations for these functions can be found in "BlockBuffer.h"

BlockBuffer::BlockBuffer(char blockType){
    
    int type;
    if(blockType == 'R'){
        type = REC;
    }
    else if(blockType == 'I'){
        type = IND_INTERNAL;
    }
    else{
        type = IND_LEAF;
    }

    int ret = BlockBuffer::getFreeBlock(type);
    this->blockNum = ret;
}

BlockBuffer::BlockBuffer(int blockNum) {
  // initialise this.blockNum with the argument
  this->blockNum = blockNum;
}

RecBuffer::RecBuffer() : BlockBuffer('R'){}
// call parent non-default constructor with 'R' denoting record block.

// calls the parent class constructor
RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

int BlockBuffer::getBlockNum(){

    return this->blockNum;
}

// load the block header into the argument pointer
int BlockBuffer::getHeader(struct HeadInfo *head) {
  //unsigned char buffer[BLOCK_SIZE];
    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    while(ret != SUCCESS){
        return ret;
    }
  // read the block at this.blockNum into the buffer
  //Disk::readBlock(buffer,this->blockNum);

  // populate the numEntries, numAttrs and numSlots fields in *head
  memcpy(&head->numSlots, bufferPtr + 24, 4);
  memcpy(&head->numEntries, bufferPtr + 16, 4);
  memcpy(&head->numAttrs, bufferPtr + 20, 4);
  memcpy(&head->rblock, bufferPtr + 12, 4);
  memcpy(&head->lblock, bufferPtr + 8, 4);

  return SUCCESS;
}

int BlockBuffer::setHeader(struct HeadInfo *head){

    unsigned char *bufferPtr;
    // get the starting address of the buffer containing the block using
    // loadBlockAndGetBufferPtr(&bufferPtr).
    int ret = BlockBuffer::loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret != SUCCESS){
        return ret;
    }

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.

    // cast bufferPtr to type HeadInfo*
    struct HeadInfo *bufferHeader = (struct HeadInfo *)bufferPtr;

    // copy the fields of the HeadInfo pointed to by head (except reserved) to
    // the header of the block (pointed to by bufferHeader)
    //(hint: bufferHeader->numSlots = head->numSlots )
    bufferHeader->blockType = head->blockType;
    bufferHeader->pblock = head->pblock;
    bufferHeader->rblock = head->rblock;
    bufferHeader->lblock = head->lblock;
    bufferHeader->numAttrs = head->numAttrs;
    bufferHeader->numEntries = head->numEntries;
    bufferHeader->numSlots = head->numSlots;

    // update dirty bit by calling StaticBuffer::setDirtyBit()
    // if setDirtyBit() failed, return the error code
    ret = StaticBuffer::setDirtyBit(this->blockNum);
    return ret;

    // return SUCCESS;
}


// load the record at slotNum into the argument pointer
int RecBuffer::getRecord(union Attribute *rec, int slotNum) {
  struct HeadInfo head;

  // get the header using this.getHeader() function
  this->getHeader(&head);

  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;

  // read the block at this.blockNum into a buffer
  //unsigned char buffer[BLOCK_SIZE];
  //Disk::readBlock(buffer,this->blockNum);
  unsigned char *bufferPtr;
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  while(ret != SUCCESS){
    return ret;
  }

  /* record at slotNum will be at offset HEADER_SIZE + slotMapSize + (recordSize * slotNum)
     - each record will have size attrCount * ATTR_SIZE
     - slotMap will be of size slotCount
  */
 int slotMapSize = slotCount;
  int recordSize = attrCount * ATTR_SIZE;
  int offset = HEADER_SIZE + slotMapSize + (recordSize * slotNum);
  unsigned char *slotPointer = bufferPtr + offset;

  // load the record into the rec data structure
  memcpy(rec, slotPointer, recordSize);

  return SUCCESS;
}

int RecBuffer::setRecord(union Attribute *rec, int slotNum) {
  
  unsigned  char *bufferPtr;
  int ret = BlockBuffer::loadBlockAndGetBufferPtr(&bufferPtr);

  if(ret != SUCCESS){
    return ret;
  }
  
  
  
  struct HeadInfo head;
  // get the header using this.getHeader() function
  BlockBuffer::getHeader(&head);

  int attrCount = head.numAttrs;
  int slotCount = head.numSlots;

  if(slotNum < 0 || slotNum >= slotCount){
    return E_OUTOFBOUND;
  }

  // read the block at this.blockNum into a buffer
  //unsigned char buffer[BLOCK_SIZE];
  //Disk::readBlock(buffer,this->blockNum);

  /* record at slotNum will be at offset HEADER_SIZE + slotMapSize + (recordSize * slotNum)
     - each record will have size attrCount * ATTR_SIZE
     - slotMap will be of size slotCount
  */
 int slotMapSize = slotCount;
  int recordSize = attrCount * ATTR_SIZE;
  int offset = HEADER_SIZE + slotMapSize + (recordSize * slotNum);
  unsigned char *slotPointer = bufferPtr + offset;

  // load the record into the rec data structure
  memcpy(slotPointer, rec, recordSize);
  //Disk::writeBlock(buffer,this->blockNum);

    StaticBuffer::setDirtyBit(this->blockNum);

  return SUCCESS;
}

int BlockBuffer::setBlockType(int blockType){

    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block
       using loadBlockAndGetBufferPtr(&bufferPtr). */
       int ret = BlockBuffer::loadBlockAndGetBufferPtr(&bufferPtr);
       if(ret != SUCCESS){
        return ret;
       }

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.

    // store the input block type in the first 4 bytes of the buffer.
    // (hint: cast bufferPtr to int32_t* and then assign it)
    // *((int32_t *)bufferPtr) = blockType;

    *((int32_t *)bufferPtr) = blockType;

    // update the StaticBuffer::blockAllocMap entry corresponding to the
    // object's block number to `blockType`.

    StaticBuffer::blockAllocMap[this->blockNum] = blockType;

    // update dirty bit by calling StaticBuffer::setDirtyBit()
    // if setDirtyBit() failed
        // return the returned value from the call

    // return SUCCESS

    ret = StaticBuffer::setDirtyBit(this->blockNum);
    return ret;
}

int BlockBuffer::getFreeBlock(int blockType){

    // iterate through the StaticBuffer::blockAllocMap and find the block number
    // of a free block in the disk.

    int freeblock = -1;

    for(int i = 0 ; i < DISK_BLOCKS ; i++){
        if(StaticBuffer::blockAllocMap[i] == UNUSED_BLK){
            freeblock = i;
            break;
        }
    }

    // if no block is free, return E_DISKFULL.
    if(freeblock == DISK_BLOCKS){
        return E_DISKFULL;
    }

    // set the object's blockNum to the block number of the free block.
    this->blockNum = freeblock;

    // find a free buffer using StaticBuffer::getFreeBuffer() .

    int bufferNum = StaticBuffer::getFreeBuffer(freeblock);

    // initialize the header of the block passing a struct HeadInfo with values
    // pblock: -1, lblock: -1, rblock: -1, numEntries: 0, numAttrs: 0, numSlots: 0
    // to the setHeader() function.

    struct HeadInfo head;
    head.pblock = -1;
    head.lblock = -1;
    head.rblock = -1;
    head.numEntries = 0;
    head.numAttrs = 0;
    head.numSlots = 0;

    BlockBuffer::setHeader(&head);

    // update the block type of the block to the input block type using setBlockType().

    BlockBuffer::setBlockType(blockType);

    // return block number of the free block.

    return freeblock;
}



int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr) {
  /* check whether the block is already present in the buffer
    using StaticBuffer.getBufferNum() */
  int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

  if (bufferNum != E_BLOCKNOTINBUFFER) {
    for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++) {
      StaticBuffer::metainfo[bufferIndex].timeStamp++;
    }
    StaticBuffer::metainfo[bufferNum].timeStamp = 0;
  } else {

    bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

    if (bufferNum == E_OUTOFBOUND) {
      return E_OUTOFBOUND; // the blockNum is invalid
    }

    Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
  }
  *buffPtr=StaticBuffer::blocks[bufferNum];
  return SUCCESS;

  // // store the pointer to this buffer (blocks[bufferNum]) in *buffPtr
  // *buffPtr = StaticBuffer::blocks[bufferNum];

  // return SUCCESS;
}

/* used to get the slotmap from a record block
NOTE: this function expects the caller to allocate memory for `*slotMap`
*/
int RecBuffer::getSlotMap(unsigned char *slotMap) {
  unsigned char *bufferPtr;

  // get the starting address of the buffer containing the block using loadBlockAndGetBufferPtr().
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }

  struct HeadInfo head;
  BlockBuffer::getHeader(&head);
  // get the header of the block using getHeader() function

  int slotCount = head.numSlots;

  // get a pointer to the beginning of the slotmap in memory by offsetting HEADER_SIZE
  unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;
  memcpy(slotMap, slotMapInBuffer, slotCount);

  // copy the values from `slotMapInBuffer` to `slotMap` (size is `slotCount`)

  return SUCCESS;
}

int RecBuffer::setSlotMap(unsigned char *slotMap) {
    unsigned char *bufferPtr;
    /* get the starting address of the buffer containing the block using
       loadBlockAndGetBufferPtr(&bufferPtr). */

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if(ret != SUCCESS){
        return ret;
    }

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.

    // get the header of the block using the getHeader() function

    struct HeadInfo head;
    BlockBuffer::getHeader(&head);

    int numSlots = head.numSlots;

    // the slotmap starts at bufferPtr + HEADER_SIZE. Copy the contents of the
    // argument `slotMap` to the buffer replacing the existing slotmap.
    // Note that size of slotmap is `numSlots`

    memcpy(bufferPtr + HEADER_SIZE, slotMap, head.numSlots);

    // update dirty bit using StaticBuffer::setDirtyBit
    // if setDirtyBit failed, return the value returned by the call

    ret = StaticBuffer::setDirtyBit(this->blockNum);
    return ret;

    // return SUCCESS
}

int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType) {

    double diff;
    // if attrType == STRING
    //     diff = strcmp(attr1.sval, attr2.sval)

    // else
    //     diff = attr1.nval - attr2.nval

    /*
    if diff > 0 then return 1
    if diff < 0 then return -1
    if diff = 0 then return 0
    */
   if(attrType == STRING){
    diff = strcmp(attr1.sVal, attr2.sVal);
   }
   else{
    diff = attr1.nVal - attr2.nVal;
   }

    if(diff < 0){
        return -1;
    }
    else if(diff > 0){
        return 1;
    }
    else{
        return 0;
    }
   
}

void BlockBuffer::releaseBlock(){

    // if blockNum is INVALID_BLOCKNUM (-1), or it is invalidated already, do nothing

    // else
        /* get the buffer number of the buffer assigned to the block
           using StaticBuffer::getBufferNum().
           (this function return E_BLOCKNOTINBUFFER if the block is not
           currently loaded in the buffer)
            */

        // if the block is present in the buffer, free the buffer
        // by setting the free flag of its StaticBuffer::tableMetaInfo entry
        // to true.

        // free the block in disk by setting the data type of the entry
        // corresponding to the block number in StaticBuffer::blockAllocMap
        // to UNUSED_BLK.

        // set the object's blockNum to INVALID_BLOCK (-1)

        
        if(this->blockNum == INVALID_BLOCKNUM ){
            return;
        }

        int bufferNum = StaticBuffer::getBufferNum(this->blockNum);
        if(bufferNum != E_BLOCKNOTINBUFFER){
            StaticBuffer::metainfo[bufferNum].free = true;
        }
        StaticBuffer::blockAllocMap[this->blockNum] = UNUSED_BLK;
        this->blockNum = INVALID_BLOCKNUM;
       
}




