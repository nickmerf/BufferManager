/**
 * Nick Merfeld nmerfeld
 * Uilliam Lawless ulawless
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) { // numBufs = bufs

	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++)
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
	delete [] bufPool;
}

// Advance clock to next frame in buffer pool 
void BufMgr::advanceClock()
{
	clockHand++;
	clockHand = clockHand % (this->numBufs);
}

/*
  Allocates a free frame using the clock algorithm; if necessary, writing a dirty page back to disk. 
  Throws BufferExceededException if all buffer frames are pinned. This private method will get called 
  by the readPage() and allocPage() methods described below. Make sure that if the buffer frame allocated 
  has a valid page in it, you remove the appropriate entry from the hash table.
*/
void BufMgr::allocBuf(FrameId & frame) 
{
  
}

/*
  First check whether the page is already in the buffer pool by invoking the lookup() method, which may throw 
  HashNotFoundException when page is not in the buffer pool, on the hashtable to get a frame number. There are 
  two cases to be handled depending on the outcome of the lookup() call: 
  1: Page is not in the buffer pool. Call allocBuf() to allocate a buffer frame and then call the method
      ﬁle->readPage() to read the page from disk into the buffer pool frame. Next, insert the page into the 
      hashtable. Finally, invoke Set() on the frame to set it up properly. Set() will leave the pinCnt for the 
      page set to 1. Return a pointer to the frame containing the page via the page parameter. 
  2: Page is in the buffer pool. In this case set the appropriate refbit, increment the pinCnt for the page, 
      and then return a pointer to the frame containing the page via the page parameter.
*/
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
  // Desired frame number
  FrameId fid;
  FrameId & frame = fid;
  try{
    hashTable->lookup(file, pageNo, frame);
  }
  // Page is not in the buffer pool
  catch(HashNotFoundException & e){
    // Call allocBuf() to allocate a buffer frame
    try{
      allocBuf(frame);
    }
    catch(BufferExceededException & e){
        return;
    }
    // Call the method ﬁle->readPage() to read the page from disk into the buffer pool frame
    Page p = file->readPage(pageNo);
    // If page is valid insert it into the hashtable
    hashTable->insert(file, pageNo, frame);
    // invoke Set() on the frame to set it up properly
    bufDescTable->Set(file, pageNo);
    // Return a pointer to the frame containing the page via the page parameter.
    page = &p;
  }
  // Page is in the buffer pool
  // set the appropriate refbit
  
  // increment the pinCnt for the page
  
  // return a pointer to the frame containing the page
  
}

/*
  Decrements the pinCnt of the frame containing (ﬁle, PageNo) and, if dirty == true, sets the dirty bit. 
  Throws PAGENOTPINNED if the pin count is already 0. Does nothing if
  page is not found in the hash table lookup.
*/
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
  
}

/*
  Should scan bufTable for pages belonging to the ﬁle. For each page encountered it should: 
  (a)if the page is dirty,call ﬁle->writePage()to ﬂush the page to disk and then set the dirty bit for the page 
      tofalse
  (b)remove the page from the hashtable(whether the page is clean or dirty) and 
  (c)invoke the Clear() method of BufDesc for the page frame. Throws PagePinnedException if some page of the ﬁle is pinned. 
      Throws BadBufferException if an invalid page belonging to the ﬁle is encountered. 
*/
void BufMgr::flushFile(const File* file) 
{
  
}

/*
  The ﬁrst step in this method is to to allocate an empty page in the speciﬁed ﬁle by invoking the ﬁle->allocatePage()
  method. This method will return a newly allocated page. Then allocBuf() is called to obtain a buffer pool frame.   
  Next, an entry is inserted into the hash table and Set() is invoked on the frame to set it up properly. The method 
  returns boththepagenumberofthenewlyallocatedpagetothecallerviathepageNoparameter and a pointer to the buffer frame   
  allocated for the page via the page parameter.
*/
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
  
}

/*
  This method deletes a particular page from ﬁle. Before deleting the page from ﬁle, it makes sure that if the page to 
  be deleted is allocated a frame in the buffer pool,that frame is freed and correspondingly entry from hash table 
  is also removed.
*/
void BufMgr::disposePage(File* file, const PageId PageNo)
{
    
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
