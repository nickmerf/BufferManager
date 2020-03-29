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

/**
  * Allocate a free frame.  
  *
  * @param frame   	Frame reference, frame ID of allocated frame returned via this variable
  * @throws BufferExceededException If no such buffer is found which can be allocated
  */
void BufMgr::allocBuf(FrameId & frame) 
{
	FrameId initial = clockHand;
  if(!bufDescTable[clockHand].valid) {
			frame = bufDescTable[clockHand].frameNo;
      return;
  }
  else if(bufDescTable[clockHand].pinCnt ==0 && !bufDescTable[clockHand].refbit) {
    // if frame dirty write back to disk
    if(bufDescTable[clockHand].dirty){
      // get the dirty frame's page
      Page & newPage = bufPool[clockHand];
      // write the page to the appropriate page on disk
      bufDescTable[clockHand].file->writePage(newPage);
      // remove entry from the hashtable
      hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
      // invoke the Clear() method of BufDesc for the page frame
      bufDescTable[clockHand].Clear();
    }
    frame = bufDescTable[clockHand].frameNo;
    return;
  }
	advanceClock();
	while(initial != clockHand) {
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
      // if frame dirty write back to disk
      if(bufDescTable[clockHand].dirty){
        // get the dirty frame's page
        Page & newPage = bufPool[clockHand];
        // write the page to the appropriate page on disk
        bufDescTable[clockHand].file->writePage(newPage);
        // remove entry from the hashtable
        hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
        // invoke the Clear() method of BufDesc for the page frame
        bufDescTable[clockHand].Clear();
      }
      frame = bufDescTable[clockHand].frameNo;
      break;
    }
  }
  if(initial == clockHand) {
    throw BufferExceededException();
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
  FrameId frame;
  try{
    hashTable->lookup(file, pageNo, frame);
  }
  // Page is not in the buffer pool
  catch(HashNotFoundException & e){
    std:: cout << "Not Found" << "\n";
    // Call allocBuf() to allocate a buffer frame
    try{
      allocBuf(frame);
    }
    catch(BufferExceededException & e){
        return;
    }   
    // Call the method file->readPage() to read the page from disk into the buffer pool frame
    Page p = file->readPage(pageNo);
    // If page is valid insert it into the hashtable
    bufPool[frame] = p;     
    hashTable->insert(file, pageNo, frame);
    // invoke Set() on the frame to set it up properly
    bufDescTable[frame].Set(file, pageNo);
    // Return a pointer to the frame containing the page via the page parameter.
    page = &bufPool[frame];
    return;
  }
  // Page is in the buffer pool
  // set the appropriate refbit
  bufDescTable[frame].refbit = true;
  // increment the pinCnt for the page
  bufDescTable[frame].pinCnt++;
  // return a pointer to the frame containing the page
  page = &bufPool[frame];
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
  // Check if the page that we want to unpin is in the buffer pool
  try{
    hashTable->lookup(file, pageNo, frame);
  }
  // Page is not in the buffer pool
  catch(HashNotFoundException & e){
    return;
  }
  // if pinCount is already zero it cannot be decremented any more, throw error
  if(bufDescTable[frame].pinCnt == 0){
    throw PageNotPinnedException(file->filename(), pageNo, frame);
  }
  else{
    bufDescTable[frame].pinCnt--;
  }
  // if dirty is true set the dirty bit of the page/frame
  if(dirty == true){
    bufDescTable[frame].dirty = true;
  }
}

/**
  * Writes out all dirty pages of the file to disk.
  * All the frames assigned to the file need to be unpinned from buffer pool before this function can be successfully called.
  * Otherwise Error returned.
  *
  * @param file   	File object
  * @throws  PagePinnedException If any page of the file is pinned in the buffer pool 
  * @throws BadBufferException If any frame allocated to the file is found to be invalid
  */
void BufMgr::flushFile(const File* file) 
{
  // scan bufTable for pages belonging to the ﬁle
  for(unsigned int frameNo = 0; frameNo < numBufs; frameNo++){
    BufDesc & frame= bufDescTable[frameNo];
    // If the page belongs to the file
    if(frame.file == file){
      // if an invalid page belonging to the ﬁle is encountered throw the exception
      if(frame.pageNo == 0){
        throw BadBufferException(frameNo, frame.dirty, frame.valid, frame.refbit);
      }
      PageId pageNo = frame.pageNo;
      if(frame.pinCnt > 0){
        throw PagePinnedException(file->filename(), pageNo, frameNo);
      }
      // if the page is dirty
      if(frame.dirty == true){
        // get the frame's page
        Page & newPage = bufPool[frameNo];
        // write the page to the appropriate page on disk
        bufDescTable[frameNo].file->writePage(newPage);
      }
      // remove the page from the hashtable
      hashTable->remove(file, pageNo);
      // invoke the Clear() method of BufDesc for the page frame
      bufDescTable[frameNo].Clear();
    }
  }
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
  // the frame the Page will be allocated in
  FrameId newFrame;
  // the new Page to be added to the buffer
  Page newPage = file->allocatePage();
  // find a free frame for the page
  allocBuf(newFrame);
  // insert the page into the frame
  bufPool[newFrame] = newPage; 
  // return the new page
  page = &bufPool[newFrame];
  // return the new page number
  pageNo = page->page_number(); 
  // insert the Page into the hash table
	hashTable->insert(file, pageNo, newFrame);
  // initiate the frame
  bufDescTable[newFrame].Set(file, pageNo);
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
