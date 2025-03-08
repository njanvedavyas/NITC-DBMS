#include "OpenRelTable.h"
#include <cstdlib>
#include <cstring>
#include <cstdio>

OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

OpenRelTable::OpenRelTable() {

  // initialize relCache and attrCache with nullptr
  for (int i = 0; i < MAX_OPEN; ++i) {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
    tableMetaInfo[i].free = true;
  }

  /************ Setting up Relation Cache entries ************/
  // (we need to populate relation cache with entries for the relation catalog
  //  and attribute catalog.)

  /**** setting up Relation Catalog relation in the Relation Cache Table****/
  RecBuffer relCatBlock(RELCAT_BLOCK);

  Attribute relCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

  struct RelCacheEntry relCacheEntry;
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;

  // allocate this on the heap because we want it to persist outside this function
  RelCacheTable::relCache[RELCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry;

  /**** setting up Attribute Catalog relation in the Relation Cache Table ****/

  // set up the relation cache entry for the attribute catalog similarly
  // from the record at RELCAT_SLOTNUM_FOR_ATTRCAT
  
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;

  RelCacheTable::relCache[ATTRCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[ATTRCAT_RELID]) = relCacheEntry;

  // set the value at RelCacheTable::relCache[ATTRCAT_RELID]

//   relCatBlock.getRecord(relCatRecord, 2);
//   RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
//   relCacheEntry.recId.block = RELCAT_BLOCK;
//   relCacheEntry.recId.slot = 2;

//   RelCacheTable::relCache[2] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
//   *(RelCacheTable::relCache[2]) = relCacheEntry;


  /************ Setting up Attribute cache entries ************/
  // (we need to populate attribute cache with entries for the relation catalog
  //  and attribute catalog.)

  /**** setting up Relation Catalog relation in the Attribute Cache Table ****/
  RecBuffer attrCatBlock(ATTRCAT_BLOCK);

  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

  struct AttrCacheEntry *head, *last;

  for(int i = 0 ; i<6 ;i++){
    attrCatBlock.getRecord(attrCatRecord,i);
        struct AttrCacheEntry* attrCacheEntry = (struct AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
        AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry->attrCatEntry);
        attrCacheEntry->recId.block = ATTRCAT_BLOCK;
        attrCacheEntry->recId.slot = i;

    if(i == 0){
        head = attrCacheEntry;
        last = attrCacheEntry;
    }
    else{
        last->next =attrCacheEntry;
        last = last->next;
    }
    

  }
  last->next = nullptr;
  AttrCacheTable::attrCache[RELCAT_RELID] = head;

  // iterate through all the attributes of the relation catalog and create a linked
  // list of AttrCacheEntry (slots 0 to 5)
  // for each of the entries, set
  //    attrCacheEntry.recId.block = ATTRCAT_BLOCK;
  //    attrCacheEntry.recId.slot = i   (0 to 5)
  //    and attrCacheEntry.next appropriately
  // NOTE: allocate each entry dynamically using malloc

  // set the next field in the last entry to nullptr

  //AttrCacheTable::attrCache[RELCAT_RELID] = /* head of the linked list */;

  /**** setting up Attribute Catalog relation in the Attribute Cache Table ****/

  // set up the attributes of the attribute cache similarly.
  // read slots 6-11 from attrCatBlock and initialise recId appropriately
  for(int i=6 ; i<12 ; i++){
    attrCatBlock.getRecord(attrCatRecord,i);
    struct AttrCacheEntry *attrCacheEntry = (struct AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry->attrCatEntry);
    attrCacheEntry->recId.block = ATTRCAT_BLOCK;
    attrCacheEntry->recId.slot = i;

    if(i==6){
        head = attrCacheEntry;
        last = attrCacheEntry;
    }
    else{
        last->next = attrCacheEntry;
        last= last ->next;
    }
  }
  last->next = nullptr;
  AttrCacheTable::attrCache[ATTRCAT_RELID] = head;


//   for(int i=12 ; i<18 ; i++){
//     attrCatBlock.getRecord(attrCatRecord,i);
//     AttrCacheEntry *attrCacheEntry = (struct AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
//     AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry->attrCatEntry);
//     attrCacheEntry->recId.block = ATTRCAT_BLOCK;
//     attrCacheEntry->recId.slot = i;

//     if(i==12){
//         head = attrCacheEntry;
//         last = attrCacheEntry;
//     }
//     else{
//         last->next = attrCacheEntry;
//         last= last ->next;
//     }
//   }
//   last->next = nullptr;
//   AttrCacheTable::attrCache[2] = head;

  // set the value at AttrCacheTable::attrCache[ATTRCAT_RELID]

  tableMetaInfo[RELCAT_RELID].free = false;
  tableMetaInfo[ATTRCAT_RELID].free = false;
  strcpy(tableMetaInfo[RELCAT_RELID].relName,"RELATIONCAT");
  strcpy(tableMetaInfo[ATTRCAT_RELID].relName,"ATTRIBUTECAT");
}

OpenRelTable::~OpenRelTable() {
  // free all the memory that you allocated in the constructor

  // close all the open relations from rel-id = 2 onwards
    for(int i = 2; i<MAX_OPEN; i++){
        if(!tableMetaInfo[i].free){
            OpenRelTable::closeRel(i);
        }
    }


  for(int i = 0 ; i < MAX_OPEN ; i++){
    if(RelCacheTable::relCache[i] != nullptr){
        free(RelCacheTable::relCache[i]);
        RelCacheTable::relCache[i] = nullptr;
    }

    if(AttrCacheTable::attrCache[i] != nullptr){
        struct AttrCacheEntry *attrCacheEntry = AttrCacheTable::attrCache[i];
        while(attrCacheEntry != nullptr){
            struct AttrCacheEntry *tempCacheEntry = attrCacheEntry;
            attrCacheEntry = attrCacheEntry->next;
            free(tempCacheEntry);
        }
        AttrCacheTable::attrCache[i] = nullptr;
    }
  }
}

/* This function will open a relation having name `relName`.
Since we are currently only working with the relation and attribute catalog, we
will just hardcode it. In subsequent stages, we will loop through all the relations
and open the appropriate one.
*/
int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {

  /* traverse through the tableMetaInfo array,
    find the entry in the Open Relation Table corresponding to relName.*/

  // if found return the relation id, else indicate that the relation do not
  // have an entry in the Open Relation Table.
  for(int i = 0 ; i < MAX_OPEN ; i++){
    if(strcmp(tableMetaInfo[i].relName,relName) == 0 and tableMetaInfo[i].free == false){
        return i;
    }
  }
  return E_RELNOTOPEN;
}


int OpenRelTable::getFreeOpenRelTableEntry() {

  /* traverse through the tableMetaInfo array,
    find a free entry in the Open Relation Table.*/

    for(int i = 2 ; i < MAX_OPEN ; i++){
        if(tableMetaInfo[i].free){
            return i;
        }
    }
    return E_CACHEFULL;
  // if found return the relation id, else return E_CACHEFULL.
}

int OpenRelTable::openRel(char relName[ATTR_SIZE]){
    // if relation with relName already has an entry in the Open Relation Table, return the rel-id
    int relId = getRelId(relName);
    if(relId != E_RELNOTOPEN){
        return relId;
    }

    // find a free slot in the Open Relation Table
    relId = getFreeOpenRelTableEntry();
    if(relId < 0){
        return E_CACHEFULL;
    }

    /******* Setting up Relation Cache entry for the free slot *********/
    // search for the entry with the relation name, relName, in the Relation Catalog using linearSearch()
    Attribute relationName;
    strcpy(relationName.sVal, relName);
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    char relCatAttrRelName[ATTR_SIZE];
    strcpy(relCatAttrRelName, RELCAT_ATTR_RELNAME);

    // relcatRecId stores the rec-id of the relation `relName` in the Relation Catalog.
    RecId relCatRecId = BlockAccess::linearSearch(RELCAT_RELID, relCatAttrRelName, relationName, EQ);

    // if the relation is not found in the Relation Catalog.
    if(relCatRecId.block == -1 && relCatRecId.slot == -1){
        return E_RELNOTEXIST;
    }

    /*
        Read the record entry corresponding to the relcatRecId and create a relCacheEntry
        on it using RecBuffer::getRecord() and RecCacheTable::recordToRelCatEntry().
        Update the recId field of this Relation Cache entry to relcatRecId.
        Use the relation cache entry to set the relId-th entry of the RelCacheTable.

        NOTE: make sure to allocate memory for the RelCacheEntry using malloc()
    */
    RecBuffer relCatBlock(relCatRecId.block); // here instead we can also use RELCAT_RELID
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    relCatBlock.getRecord(relCatRecord, relCatRecId.slot);
    struct RelCacheEntry relCacheEntry;
    RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
    relCacheEntry.recId = relCatRecId;
    RelCacheTable::relCache[relId] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
    *(RelCacheTable::relCache[relId]) = relCacheEntry;

    /******** Setting up Attribute Cache entry for the relation *********/

    // let listHead be used to hold the head of the linked list of attrCache entries.
    AttrCacheEntry* listHead, *current;

    /*
        Iterate over all the entries in the Attribute Catalog corresponding to each
        attribute of the relation relName by multiple calls of BlockAccess::linearSearch().
        Care should be take to reset the searchIndex of the relation, ATTRCAT_RELID,
        corresponding to Attribute Catalog before the first call to linearSearch().
    */
    RecId attrCatRecord;
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    for(int i = 0; i<relCacheEntry.relCatEntry.numAttrs; i++){
        /* let attrcatRecId store a valid record id an entry of the relation, relName,
           in the Attribute Catalog.
        */
        RecId attrcatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, relCatAttrRelName, relationName, EQ);
        /*  read the record entry corresponding to attrcatRecId and create an
            Attribute Cache entry on it using RecBuffer::getRecord() and
            AttrCacheTable::recordToAttrCatEntry().
            update the recId field of this Attribute Cache entry to attrcatRecId.
            add the Attribute Cache entry to the linked list of listHead .
        */
      // NOTE: make sure to allocate memory for the AttrCacheEntry using malloc()
      RecBuffer attrCatBlock(attrcatRecId.block);
      Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
      attrCatBlock.getRecord(attrCatRecord, attrcatRecId.slot);
      struct AttrCacheEntry* attrCacheEntry = (struct AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
      AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry->attrCatEntry);
      attrCacheEntry->recId = attrcatRecId;

      if(i==0){
        listHead = attrCacheEntry;
        current = attrCacheEntry;
      }
      else{
        current->next = attrCacheEntry;
        current = attrCacheEntry;
      }
    }

    current->next = nullptr;

    // set the relId-th entry of the AttrCacheTable to listHead
    AttrCacheTable::attrCache[relId] = listHead;


    /********* Setting up metadata in the Open Relation Table for the relation *********/
    // update the relIdth entry of the tableMetaInfo with free as false and relName as the input.
    OpenRelTable::tableMetaInfo[relId].free = false;
    strcpy(OpenRelTable::tableMetaInfo[relId].relName, relName);

    return relId;

}



int OpenRelTable::closeRel(int relId) {
  if (relId == RELCAT_RELID || relId == ATTRCAT_RELID) {
    return E_NOTPERMITTED;
  }

  if (relId < 0 || relId >= MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  if (tableMetaInfo[relId].free == true) {
    return E_RELNOTOPEN;
  }

    /****** Releasing the Relation Cache entry of the relation ******/

  if (RelCacheTable::relCache[relId]->dirty =true)
  {

    /* Get the Relation Catalog entry from RelCacheTable::relCache
    Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */

    Attribute relCatRecord[RELCAT_NO_ATTRS];
    RelCacheTable::relCatEntryToRecord(&RelCacheTable::relCache[relId]->relCatEntry,relCatRecord);


    // declaring an object of RecBuffer class to write back to the buffer
    RecBuffer relCatBlock(RelCacheTable::relCache[relId]->recId.block);
    relCatBlock.setRecord(relCatRecord, RelCacheTable::relCache[relId]->recId.slot);

    // Write back to the buffer using relCatBlock.setRecord() with recId.slot
  }

  /****** Releasing the Attribute Cache entry of the relation ******/

  // free the memory allocated in the attribute caches which was
  // allocated in the OpenRelTable::openRel() function

  // (because we are not modifying the attribute cache at this stage,
  // write-back is not required. We will do it in subsequent
  // stages when it becomes needed)
  // free the memory allocated in the relation and attribute caches which was
  // allocated in the OpenRelTable::openRel() function

  free(RelCacheTable::relCache[relId]);
  AttrCacheEntry *entry, *temp;
  entry = AttrCacheTable::attrCache[relId];
  while(entry !=  nullptr){
    temp = entry;
    entry = entry ->next;
    free(temp);
  }

  tableMetaInfo[relId].free = true;
  RelCacheTable::relCache[relId] = nullptr;
  AttrCacheTable::attrCache[relId] = nullptr;

  // update `tableMetaInfo` to set `relId` as a free slot
  // update `relCache` and `attrCache` to set the entry at `relId` to nullptr

  return SUCCESS;
}
