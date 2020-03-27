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
	FrameId intial = clockHand;
	advanceClock();
	while(intial != clockHand) {
		if(!bufDescTable[clockHand].valid) {
			frame = bufDescTable[clockHand].frameNo;
                        break;
                }
                if(bufDescTable[clockHand].refbit) {
                        bufDescTable[clockHand].refbit = false;
                        advanceClock();
                        continue;
                }
                if(bufDescTable[clockHand].pinCnt > 0) {
                        advanceClock();
                        continue;
                }
                if(bufDescTable[clockHand].pinCnt ==0 && !bufDescTable[clockHand].refbit) {
                        frame = bufDescTable[clockHand].frameNo;
                        break;
                }
        }

        if(initial == clockHand) {
                throw BufferExceededException;
        }
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
    // Call the method ?le->readPage() to read the page from disk into the buffer pool frame
    Page p = file->readPage(pageNo);
    // If page is valid insert it into the hashtable
    hashTable->insert(file, pageNo, frame);
    // invoke Set() on the frame to set it up properly
    bufDescTable->Set(file, pageNo);
    // Return a pointer to the frame containing the page via the page parameter.
    page = &p;
    return;
  }
  // Page is in the buffer pool
  // set the appropriate refbit
  bufDescTable[frame].refbit = true;
  // increment the pinCnt for the page
  bufDescTable[frame].pinCnt++;
  // return a pointer to the frame containing the page
  Page p = file->readPage(pageNo);
  page =&p;
}

/**
* Unpin a page from memory since it is no longer required for it to remain in memory.
*
* @param file   File object
* @param PageNo  Page number
* @param dirty	True if the page to be unpinned needs to be marked dirty	
   * @throws  PageNotPinnedException If the page is not already pinned
	 */
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
  FrameId frame;
  try{
    hashTable->lookup(file, pageNo, frame);
  }
  // Page is not in the buffer pool
  catch(HashNotFoundException & e){
    return;
  }
  if(bufDescTable[frame].pinCnt == 0){
    throw PageNotPinnedException(file->filename(), pageNo, frame);
  }
  else{
    bufDescTable[frame].pinCnt--;
  }
  // Not sure if this should be inside the else (if the pin count is already 0 should the dirt bit still be affected?)
  if(dirty == true){
    bufDescTable[frame].dirty = true;
  }
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

/**
 * allocates a new page for a file and adds it to the buffer
 * returns the page number of the newly allocated page
 * @param file   pointer to file to allocate a new page for
 * @param pageNo page number of newly allocated page
 * @param page   allocated page
 */
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
{
	FrameId newFrame;
	Page newPage = file->allocatePage();
	page = &newPage;
	allocBuf(newFrame);
	hashTable->insert(file, pageNo, newFrame);
	bufDescTable->Set(file, pageNo);

	pageNo = page->page_number(); // new page number
	bufPool[newFrame] = newPage; // new frame
}

/**
  * Delete page from file and also from buffer pool if present.
  * Since the page is entirely deleted from file, its unnecessary to see if the page is dirty.
  *
  * @param file   	File object
  * @param PageNo  Page number
*/
void BufMgr::disposePage(File* file, const PageId PageNo)
{  
  FrameId frame;     
  try{ 
    hashTable->lookup(file, PageNo, frame);
  }
  // Page is not in the buffer pool
  catch(HashNotFoundException & e){
    // deletes a particular page from file
    file->deletePage(PageNo);
    return;
  } 
  // if the page to be deleted is allocated a frame in the buffer pool
  // frame is freed  
  // and correspondingly entry from hash table is also removed.
  hashTable->remove(file,PageNo);
  bufDescTable[frame].Clear();  
  // deletes a particular page from file
  file->deletePage(PageNo); 
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
