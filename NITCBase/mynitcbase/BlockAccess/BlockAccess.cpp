#include "BlockAccess.h"

#include <cstring>

RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op) {
    // get the previous search index of the relation relId from the relation cache
    // (use RelCacheTable::getSearchIndex() function)
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId,&prevRecId);

    int block=-1;
    int slot=-1;

    // let block and slot denote the record id of the record being currently checked

    // if the current search index record is invalid(i.e. both block and slot = -1)
    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        // (no hits from previous search; search should start from the
        // first record itself)

        // get the first record block of the relation from the relation cache
        // (use RelCacheTable::getRelCatEntry() function of Cache Layer)
        RelCatEntry relCatBuf;
        RelCacheTable::getRelCatEntry(relId, &relCatBuf);

        block = relCatBuf.firstBlk;
        slot = 0;

        // block = first record block of the relation
        // slot = 0
    }
    else
    {
        // (there is a hit from previous search; search should start from
        // the record next to the search index record)
        block = prevRecId.block;
        slot = prevRecId.slot + 1;
        // block = search index's block
        // slot = search index's slot + 1
    }

    /* The following code searches for the next record in the relation
       that satisfies the given condition
       We start from the record id (block, slot) and iterate over the remaining
       records of the relation
    */
    while (block != -1)
    {
        /* create a RecBuffer object for block (use RecBuffer Constructor for
           existing block) */
           RecBuffer Bufferblock(block);
           struct HeadInfo head;
           Bufferblock.getHeader(&head);
           Attribute recordblock[head.numAttrs];
           Bufferblock.getRecord(recordblock,slot);
           unsigned char slotMap[head.numSlots];
           Bufferblock.getSlotMap(slotMap);

        // get the record with id (block, slot) using RecBuffer::getRecord()
        // get header of the block using RecBuffer::getHeader() function
        // get slot map of the block using RecBuffer::getSlotMap() function

        // If slot >= the number of slots per block(i.e. no more slots in this block)

        if(slot >= head.numSlots)
        {
            // update block = right block of block
            block = head.rblock;
            slot = 0;
            continue;  
        }

        // if slot is free skip the loop
        // (i.e. check if slot'th entry in slot map of block contains SLOT_UNOCCUPIED)
        if(slotMap[slot] == SLOT_UNOCCUPIED)
        {
            // increment slot and continue to the next record slot
            slot++;
            continue;
        }

        // compare record's attribute value to the the given attrVal as below:
        /*
            firstly get the attribute offset for the attrName attribute
            from the attribute cache entry of the relation using
            AttrCacheTable::getAttrCatEntry()
        */
       AttrCatEntry attrCatEntry;
       
       AttrCacheTable::getAttrCatEntry(relId,attrName,&attrCatEntry);
       Attribute cureentAttrVal = recordblock[attrCatEntry.offset];
        /* use the attribute offset to get the value of the attribute from
           current record */

        int cmpVal = compareAttrs(cureentAttrVal,attrVal,attrCatEntry.attrType);  // will store the difference between the attributes
        // set cmpVal using compareAttrs()

        /* Next task is to check whether this record satisfies the given condition.
           It is determined based on the output of previous comparison and
           the op value received.
           The following code sets the cond variable if the condition is satisfied.
        */
        if (
            (op == NE && cmpVal != 0) ||    // if op is "not equal to"
            (op == LT && cmpVal < 0) ||     // if op is "less than"
            (op == LE && cmpVal <= 0) ||    // if op is "less than or equal to"
            (op == EQ && cmpVal == 0) ||    // if op is "equal to"
            (op == GT && cmpVal > 0) ||     // if op is "greater than"
            (op == GE && cmpVal >= 0)       // if op is "greater than or equal to"
        ) {
            /*
            set the search index in the relation cache as
            the record id of the record that satisfies the given condition
            (use RelCacheTable::setSearchIndex function)
            */
            RecId recId = {block,slot};
            RelCacheTable::setSearchIndex(relId,&recId);

            return recId;
        }

        slot++;
    }

    // no record in the relation with Id relid satisfies the given condition
    RecId recid;
    recid.block = -1;
    recid.slot = -1;
    return recid;
}



int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE]){
    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute newRelationName; 
    strcpy(newRelationName.sVal,newName);   // set newRelationName with newName
    char relcatAttrRelname[ATTR_SIZE];
    strcpy(relcatAttrRelname, RELCAT_ATTR_RELNAME);
    // search the relation catalog for an entry with "RelName" = newRelationName

    RecId recId = BlockAccess::linearSearch(RELCAT_RELID, relcatAttrRelname, newRelationName, EQ);

    // If relation with name newName already exists (result of linearSearch
    //                                               is not {-1, -1})
    //    return E_RELEXIST;

    if(recId.block != -1 && recId.slot != -1){
        return E_RELEXIST;
    }


    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute oldRelationName;
    strcpy(oldRelationName.sVal, oldName);    // set oldRelationName with oldName

    // search the relation catalog for an entry with "RelName" = oldRelationName

    recId = BlockAccess::linearSearch(RELCAT_RELID, relcatAttrRelname, oldRelationName, EQ);

    // If relation with name oldName does not exist (result of linearSearch is {-1, -1})
    //    return E_RELNOTEXIST;

    if(recId.block == -1 and recId.slot == -1){
        return E_RELNOTEXIST;
    }

    /* get the relation catalog record of the relation to rename using a RecBuffer
       on the relation catalog [RELCAT_BLOCK] and RecBuffer.getRecord function
    */
    /* update the relation name attribute in the record with newName.
       (use RELCAT_REL_NAME_INDEX) */
    // set back the record value using RecBuffer.setRecord

    RecBuffer bufferBlock(RELCAT_BLOCK);
    Attribute recordBlock[RELCAT_NO_ATTRS];
    bufferBlock.getRecord(recordBlock,recId.slot);

    strcpy(recordBlock[RELCAT_REL_NAME_INDEX].sVal, newName);
    bufferBlock.setRecord(recordBlock,recId.slot);


    /*
    update all the attribute catalog entries in the attribute catalog corresponding
    to the relation with relation name oldName to the relation name newName
    */

    /* reset the searchIndex of the attribute catalog using
       RelCacheTable::resetSearchIndex() */

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    char attrcatAttrRelname[ATTR_SIZE];
    strcpy(attrcatAttrRelname, ATTRCAT_ATTR_RELNAME);

    //for i = 0 to numberOfAttributes :
    //    linearSearch on the attribute catalog for relName = oldRelationName
    //    get the record using RecBuffer.getRecord
    //
    //    update the relName field in the record to newName
    //    set back the record using RecBuffer.setRecord

    for(int i = 0 ; i < recordBlock[RELCAT_NO_ATTRIBUTES_INDEX].nVal ; i++){
        RecId recid = BlockAccess::linearSearch(ATTRCAT_RELID, attrcatAttrRelname, oldRelationName, EQ);
        RecBuffer attrBuffer(recid.block);
        Attribute attrRecord[ATTRCAT_NO_ATTRS];
        attrBuffer.getRecord(attrRecord,recid.slot);
        strcpy(attrRecord[ATTRCAT_REL_NAME_INDEX].sVal,newName);
        attrBuffer.setRecord(attrRecord,recid.slot);
    }

    return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {

    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr;    // set relNameAttr to relName
    strcpy(relNameAttr.sVal,relName);
    char relcatAttrName[ATTR_SIZE];
    strcpy(relcatAttrName, RELCAT_ATTR_RELNAME);

    // Search for the relation with name relName in relation catalog using linearSearch()
    // If relation with name relName does not exist (search returns {-1,-1})
    //    return E_RELNOTEXIST;
    RecId recId = BlockAccess::linearSearch(RELCAT_RELID, relcatAttrName, relNameAttr, EQ);

    if(recId.block == -1 and recId.slot == -1){
        return E_RELNOTEXIST;
    }

    /* reset the searchIndex of the attribute catalog using
       RelCacheTable::resetSearchIndex() */

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    /* declare variable attrToRenameRecId used to store the attr-cat recId
    of the attribute to rename */
    RecId attrToRenameRecId;
    attrToRenameRecId.block = -1;
    attrToRenameRecId.slot = -1;
    Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];
    char attrcatAttrName[ATTR_SIZE];
    strcpy(attrcatAttrName, ATTRCAT_ATTR_RELNAME);

    /* iterate over all Attribute Catalog Entry record corresponding to the
       relation to find the required attribute */
    
    while (true) {
        // linear search on the attribute catalog for RelName = relNameAttr

        // if there are no more attributes left to check (linearSearch returned {-1,-1})
        //     break;

        RecId recid = BlockAccess::linearSearch(ATTRCAT_RELID, attrcatAttrName, relNameAttr, EQ);

        if(recid.block == -1 and recid.slot == -1){
            break;
        }

        /* Get the record from the attribute catalog using RecBuffer.getRecord
          into attrCatEntryRecord */

        RecBuffer attrcatBuffer(recid.block);
        attrcatBuffer.getRecord(attrCatEntryRecord, recid.slot);

        // if attrCatEntryRecord.attrName = oldName
        //     attrToRenameRecId = block and slot of this record

        if(strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldName) == 0){
            attrToRenameRecId.block = recid.block;
            attrToRenameRecId.slot = recid.slot;
        }

        // if attrCatEntryRecord.attrName = newName
        //     return E_ATTREXIST;

        if(strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName) == 0){
            return E_ATTREXIST;
        }
    }

    // if attrToRenameRecId == {-1, -1}
    //     return E_ATTRNOTEXIST;

    if(attrToRenameRecId.block == -1 and attrToRenameRecId.slot == -1){
        return E_ATTRNOTEXIST;
    }


    // Update the entry corresponding to the attribute in the Attribute Catalog Relation.
    /*   declare a RecBuffer for attrToRenameRecId.block and get the record at
         attrToRenameRecId.slot */
    //   update the AttrName of the record with newName
    //   set back the record with RecBuffer.setRecord

    RecBuffer attrTorenameBuff(attrToRenameRecId.block);
    attrTorenameBuff.getRecord(attrCatEntryRecord, attrToRenameRecId.slot);
    strcpy(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName);
    attrTorenameBuff.setRecord(attrCatEntryRecord, attrToRenameRecId.slot);

    return SUCCESS;
}

int BlockAccess::insert(int relId, Attribute *record) {
    // get the relation catalog entry from relation cache
    // ( use RelCacheTable::getRelCatEntry() of Cache Layer)

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    int blockNum = relCatEntry.firstBlk;

    // rec_id will be used to store where the new record will be inserted
    RecId rec_id;
    rec_id.block = -1;
    rec_id.slot = -1;

    int numOfSlots = relCatEntry.numSlotsPerBlk;
    int numOfAttributes = relCatEntry.numAttrs;

    int prevBlockNum = -1;

    /*
        Traversing the linked list of existing record blocks of the relation
        until a free slot is found OR
        until the end of the list is reached
    */
    while (blockNum != -1) {
        // create a RecBuffer object for blockNum (using appropriate constructor!)

        // get header of block(blockNum) using RecBuffer::getHeader() function

        // get slot map of block(blockNum) using RecBuffer::getSlotMap() function

        RecBuffer recBuffer(blockNum);
        struct HeadInfo head;
        recBuffer.getHeader(&head);
        unsigned char slotMap[head.numSlots];
        recBuffer.getSlotMap(slotMap);

        // search for free slot in the block 'blockNum' and store it's rec-id in rec_id
        // (Free slot can be found by iterating over the slot map of the block)
        /* slot map stores SLOT_UNOCCUPIED if slot is free and
           SLOT_OCCUPIED if slot is occupied) */

        /* if a free slot is found, set rec_id and discontinue the traversal
           of the linked list of record blocks (break from the loop) */

        /* otherwise, continue to check the next block by updating the
           block numbers as follows:
              update prevBlockNum = blockNum
              update blockNum = header.rblock (next element in the linked
                                               list of record blocks)
        */

       for(int i = 0; i < head.numSlots ; i++){
            if(slotMap[i] == SLOT_UNOCCUPIED){
                rec_id.block = blockNum;
                rec_id.slot = i;
                break;
            }
       }

       if(rec_id.block != -1 and rec_id.slot != -1){
            break;
       }

       prevBlockNum = blockNum;
       blockNum = head.rblock;
    }

    //  if no free slot is found in existing record blocks (rec_id = {-1, -1})
    if (rec_id.block == -1 and rec_id.slot == -1){
        // if relation is RELCAT, do not allocate any more blocks
        //     return E_MAXRELATIONS;

        if(relId == RELCAT_RELID){
            return E_MAXRELATIONS;
        }

        // Otherwise,
        // get a new record block (using the appropriate RecBuffer constructor!)
        // get the block number of the newly allocated block
        // (use BlockBuffer::getBlockNum() function)
        // let ret be the return value of getBlockNum() function call

        RecBuffer newRecBuffer;

        int ret = newRecBuffer.getBlockNum();

        if (ret == E_DISKFULL) {
            return E_DISKFULL;
        }

        // Assign rec_id.block = new block number(i.e. ret) and rec_id.slot = 0

        rec_id.block = ret;
        rec_id.slot = 0;

        /*
            set the header of the new record block such that it links with
            existing record blocks of the relation
            set the block's header as follows:
            blockType: REC, pblock: -1
            lblock
                  = -1 (if linked list of existing record blocks was empty
                         i.e this is the first insertion into the relation)
                  = prevBlockNum (otherwise),
            rblock: -1, numEntries: 0,
            numSlots: numOfSlots, numAttrs: numOfAttributes
            (use BlockBuffer::setHeader() function)
        */
       struct HeadInfo newHead;
       
       newHead.blockType = REC;
       newHead.pblock = -1;
       newHead.rblock = -1;
       newHead.lblock = prevBlockNum;
       newHead.numEntries = 0;
       newHead.numSlots = numOfSlots;
       newHead.numAttrs = numOfAttributes;

       newRecBuffer.setHeader(&newHead);

        /*
            set block's slot map with all slots marked as free
            (i.e. store SLOT_UNOCCUPIED for all the entries)
            (use RecBuffer::setSlotMap() function)
        */

       unsigned char newSlotMap[numOfSlots];
       

       for(int i = 0 ; i < numOfSlots ; i++){
            newSlotMap[i] = SLOT_UNOCCUPIED;
       }

       newRecBuffer.setSlotMap(newSlotMap);

        // if prevBlockNum != -1
        if(prevBlockNum != -1){
            // create a RecBuffer object for prevBlockNum
            // get the header of the block prevBlockNum and
            // update the rblock field of the header to the new block
            // number i.e. rec_id.block
            // (use BlockBuffer::setHeader() function)

            RecBuffer prevRecBuffer(prevBlockNum);
            struct HeadInfo prevHead;
            prevRecBuffer.getHeader(&prevHead);
            prevHead.rblock = rec_id.block;
            prevRecBuffer.setHeader(&prevHead);
        }
        else
        {
            // update first block field in the relation catalog entry to the
            // new block (using RelCacheTable::setRelCatEntry() function)
            
            relCatEntry.firstBlk = rec_id.block;
            RelCacheTable::setRelCatEntry(relId, &relCatEntry);
        }

        // update last block field in the relation catalog entry to the
        // new block (using RelCacheTable::setRelCatEntry() function)
        relCatEntry.lastBlk = rec_id.block;
        RelCacheTable::setRelCatEntry(relId, &relCatEntry);
    }

    // create a RecBuffer object for rec_id.block
    // insert the record into rec_id'th slot using RecBuffer.setRecord())

    /* update the slot map of the block by marking entry of the slot to
       which record was inserted as occupied) */
    // (ie store SLOT_OCCUPIED in free_slot'th entry of slot map)
    // (use RecBuffer::getSlotMap() and RecBuffer::setSlotMap() functions)

    // increment the numEntries field in the header of the block to
    // which record was inserted
    // (use BlockBuffer::getHeader() and BlockBuffer::setHeader() functions)

    // Increment the number of records field in the relation cache entry for
    // the relation. (use RelCacheTable::setRelCatEntry function)

    RecBuffer newRecBuff(rec_id.block);
    struct HeadInfo head;
    newRecBuff.getHeader(&head);
    newRecBuff.setRecord(record, rec_id.slot);

    unsigned char slotMap[head.numSlots];
    newRecBuff.getSlotMap(slotMap);
    slotMap[rec_id.slot] = SLOT_OCCUPIED;
    newRecBuff.setSlotMap(slotMap);

    head.numEntries += 1;
    newRecBuff.setHeader(&head);

    relCatEntry.numRecs += 1;
    RelCacheTable::setRelCatEntry(relId, &relCatEntry);

    return SUCCESS;
}


/*
NOTE: This function will copy the result of the search to the `record` argument.
      The caller should ensure that space is allocated for `record` array
      based on the number of attributes in the relation.
*/
int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
    // Declare a variable called recid to store the searched record
    RecId recId;

    recId = BlockAccess::linearSearch(relId, attrName, attrVal, EQ);

    /* search for the record id (recid) corresponding to the attribute with
    attribute name attrName, with value attrval and satisfying the condition op
    using linearSearch() */

    // if there's no record satisfying the given condition (recId = {-1, -1})
    //    return E_NOTFOUND;

    if(recId.block == -1 and recId.slot == -1){
        return E_NOTFOUND;
    }

    /* Copy the record with record id (recId) to the record buffer (record)
       For this Instantiate a RecBuffer class object using recId and
       call the appropriate method to fetch the record
    */

   RecBuffer recBuffer(recId.block);
   recBuffer.getRecord(record, recId.slot);

    return SUCCESS;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE]){
    // if the relation to delete is either Relation Catalog or Attribute Catalog, return E_NOTPERMITTED
    if(strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0){
        return E_NOTPERMITTED;
    }

    // reset the searchIndex of the relation catalog
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr;
    strcpy(relNameAttr.sVal, relName);

    char *relCatAttrRelname;
    strcpy(relCatAttrRelname,RELCAT_ATTR_RELNAME);

    // linearSearch on the relation catalog for RelName = relName
    RecId recId = BlockAccess::linearSearch(RELCAT_RELID, relCatAttrRelname, relNameAttr,EQ);
    if(recId.block == -1 && recId.slot == -1){
        return E_RELNOTEXIST;
    }

    /* Store the relation catalog */
    Attribute relCatEntryRecord[RELCAT_NO_ATTRS];
    RecBuffer recBuffer(recId.block);
    recBuffer.getRecord(relCatEntryRecord,recId.slot);

    /* get the first record block and number of attributes of the relation */
    int firstBlock = relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;  
    int numAttrs = relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;

    /***  Delete all the record blocks of the relation ***/
    int currBlock = firstBlock;
    while(currBlock != -1){
        // get the block header
        // get the next block from header
        // release the current block

        struct HeadInfo head;
        RecBuffer currRecBuffer(currBlock);
        currRecBuffer.getHeader(&head);
        currBlock = head.rblock;
        currRecBuffer.releaseBlock();
    }

    /*** 
        Deleting attributes catalog entries corresponding the relation and
        index block corresponding to the relation with relName on its attributes
    ***/
    // reset searchIndex of the attribute catalog
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    int numberOfAttributesDeleted = 0;

    // NOTE: ATTRCAT_ATTR_RELNAME = RELCAT_ATTR_RELNAME = Relname
    
    while(true){
        RecId attrCatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, relCatAttrRelname, relNameAttr, EQ);
        if(attrCatRecId.block == -1 && attrCatRecId.slot == -1){
            break;
        }

        numberOfAttributesDeleted++;

        // create a RecBuffer for the attrCatRecId.block and get header and record at attrCatRecId.slot
        RecBuffer recBuffer(attrCatRecId.block);
        struct HeadInfo head;
        recBuffer.getHeader(&head);
        Attribute record[head.numAttrs];
        recBuffer.getRecord(record, attrCatRecId.slot);
        
        // declare the rootBlock which will be used to store the root block field from the attribute catalog record.
        // int rootBlock = /* get root block from the record */

        // update the SlotMap for the block by setting the slot as SLOT_UNOCCUPIED
        unsigned char slotMap[head.numSlots];
        recBuffer.getSlotMap(slotMap);
        slotMap[attrCatRecId.slot] = SLOT_UNOCCUPIED;
        recBuffer.setSlotMap(slotMap);

        /*
            Decrement the numEntries in the header of the block corresponding to
            the attribute catalog entry and then set back the header.
        */
        head.numEntries--;
        recBuffer.setHeader(&head);

        /*
            If number of entries become 0, releaseBlock is called after fixing
            the linkedList
        */
        if(head.numEntries == 0){
            /*
                Standard Linked List Delete for a block:
                    Get header of left block and sets its rblock to rblock of
                    current block
            */

            RecBuffer leftRecBuffer(head.lblock);
            struct HeadInfo leftHead;
            leftRecBuffer.getHeader(&leftHead);
            leftHead.rblock = head.rblock;
            leftRecBuffer.setHeader(&leftHead);

            if(head.rblock != -1){
                /*
                    Get the header of the right blocl and set it's lblock to
                    this block's lbock
                */
                RecBuffer rightRecBuffer(head.rblock);
                struct HeadInfo rightHead;
                rightRecBuffer.getHeader(&rightHead);
                rightHead.lblock = head.lblock;
                rightRecBuffer.setHeader(&rightHead);
            }
            else{
                // so this is the last block being released of the relation
                /*
                    Update the Relation Catalog entry'a LastBlock field for this
                    relation (Attribute catalog) with the block number of the previous block
                 */
                 RelCatEntry relCatEntry;
                 RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relCatEntry);
                 relCatEntry.lastBlk = head.lblock;
                 RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &relCatEntry);
            }
            
            // call releaseBlock()
            recBuffer.releaseBlock();
        }

        // next part is for later stages.
          
    }

    /*** Delete the entry corresponding to the relation from relation catalog ***/

    // Fetch header of the relcat block
    struct HeadInfo head;
    recBuffer.getHeader(&head);

    // decrement the numEntries in the header and set it back
    head.numEntries--;
    recBuffer.setHeader(&head);

    // get the slotmap of relation catalog and mark the corresponding slot unoccupied
    unsigned char slotMap[head.numSlots];
    recBuffer.getSlotMap(slotMap);
    slotMap[recId.slot] = SLOT_UNOCCUPIED;
    recBuffer.setSlotMap(slotMap);

    /*** Updating the Relation Cache Table ***/

    // update relation catalog entry [number of record decreased by 1]
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatEntry);
    relCatEntry.numRecs--;
    RelCacheTable::setRelCatEntry(RELCAT_RELID, &relCatEntry);

    // update attribute catalog entry [num of records decreased by numberOfDeletedAttr]
    RelCatEntry attrCatEntry;
    RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &attrCatEntry);
    attrCatEntry.numRecs -= numberOfAttributesDeleted;
    RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &attrCatEntry);

    return SUCCESS;
}