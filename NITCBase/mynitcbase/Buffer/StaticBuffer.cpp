#include "StaticBuffer.h"

// the declarations for this class can be found at "StaticBuffer.h"

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
// declare the blockAllocMap array
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];

StaticBuffer::StaticBuffer() {

    for(int i = 0 ; i < 4 ; i++){
        Disk::readBlock(blockAllocMap + i*BLOCK_SIZE , i);
    }

  // initialise all blocks as free
  for (int bufferIndex = 0; bufferIndex < BUFFER_CAPACITY; bufferIndex++) {
    metainfo[bufferIndex].free = true;
    metainfo[bufferIndex].dirty = false;
    metainfo[bufferIndex].timeStamp = -1;
    metainfo[bufferIndex].blockNum = -1;
  }
}

/*
At this stage, we are not writing back from the buffer to the disk since we are
not modifying the buffer. So, we will define an empty destructor for now. In
subsequent stages, we will implement the write-back functionality here.
*/
StaticBuffer::~StaticBuffer() {

    for(int i = 0 ; i < 4 ; i++){
        Disk::writeBlock(blockAllocMap + i*BLOCK_SIZE, i);
    }

    for(int bufferIndex = 0 ; bufferIndex < BUFFER_CAPACITY ; bufferIndex++){
        if(metainfo[bufferIndex].free == false and metainfo[bufferIndex].dirty == true){
            Disk::writeBlock(StaticBuffer::blocks[bufferIndex], metainfo[bufferIndex].blockNum);
        }
    }
}

int StaticBuffer::getFreeBuffer(int blockNum) {
  if (blockNum < 0 || blockNum >= DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }
  int allocatedBuffer = -1, BufferwithMaxTimeStamp = -1, maxTimeStamp = -1;

  // iterate through all the blocks in the StaticBuffer
  // find the first free block in the buffer (check metainfo)
  // assign allocatedBuffer = index of the free block
  for(int i = 0 ; i < BUFFER_CAPACITY ; i++){
    if(metainfo[i].free == true and allocatedBuffer == -1){
        allocatedBuffer = i;
    }

    if(metainfo[i].free == false){
        metainfo[i].timeStamp += 1;

        if(maxTimeStamp < metainfo[i].timeStamp){
            maxTimeStamp = metainfo[i].timeStamp;
            BufferwithMaxTimeStamp = i;
        }
    }
  }

  if(allocatedBuffer == -1){
    if(metainfo[BufferwithMaxTimeStamp].dirty == true){
        Disk::writeBlock(StaticBuffer::blocks[BufferwithMaxTimeStamp],metainfo[BufferwithMaxTimeStamp].blockNum);
    }
    allocatedBuffer = BufferwithMaxTimeStamp;
  }

  metainfo[allocatedBuffer].free = false;
  metainfo[allocatedBuffer].dirty = false;
  metainfo[allocatedBuffer].blockNum = blockNum;
  metainfo[allocatedBuffer].timeStamp = 0;

  return allocatedBuffer;
}

/* Get the buffer index where a particular block is stored
   or E_BLOCKNOTINBUFFER otherwise
*/
int StaticBuffer::getBufferNum(int blockNum) {
  // Check if blockNum is valid (between zero and DISK_BLOCKS)
  // and return E_OUTOFBOUND if not valid.
  if(blockNum<0 || blockNum>DISK_BLOCKS){
    return E_OUTOFBOUND;
  }

  for(int i = 0 ; i < BUFFER_CAPACITY ; i++){
    if(metainfo[i].blockNum == blockNum and metainfo[i].free == false){
        return i;
    }
  }

  // find and return the bufferIndex which corresponds to blockNum (check metainfo)

  // if block is not in the buffer
  return E_BLOCKNOTINBUFFER;
}


int StaticBuffer::setDirtyBit(int blockNum){
    // find the buffer index corresponding to the block using getBufferNum().

    int buffNum = StaticBuffer::getBufferNum(blockNum);

    // if block is not present in the buffer (bufferNum = E_BLOCKNOTINBUFFER)
    //     return E_BLOCKNOTINBUFFER

    if(buffNum == E_BLOCKNOTINBUFFER){
        return E_BLOCKNOTINBUFFER;
    }

    // if blockNum is out of bound (bufferNum = E_OUTOFBOUND)
    //     return E_OUTOFBOUND

    if(buffNum == E_OUTOFBOUND){
        return E_OUTOFBOUND;
    }

    // else
    //     (the bufferNum is valid)
    //     set the dirty bit of that buffer to true in metainfo
    metainfo[buffNum].dirty = true;

    return SUCCESS;
    // return SUCCESS
}