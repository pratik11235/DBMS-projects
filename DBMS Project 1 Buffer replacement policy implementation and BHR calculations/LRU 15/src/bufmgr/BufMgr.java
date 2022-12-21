package bufmgr;

import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;
import java.util.HashMap;
import java.util.*;

/* revised slightly by sharma on 8/22/2022 */

/**
 * <h3>Minibase Buffer Manager</h3>
 * The buffer manager reads disk pages into a mains memory page as needed. The
 * collection of main memory pages (called frames) used by the buffer manager
 * for this purpose is called the buffer pool. This is just an array of Page
 * objects. The buffer manager is used by access methods, heap files, and
 * relational operators to read, write, allocate, and de-allocate pages.
 * policy class name has to be changed in the constructior using name of the 
 * class you have implementaed
 */
public class BufMgr implements GlobalConst {

    /** Actual pool of pages (can be viewed as an array of byte arrays). */
    protected Page[] bufpool;

    /** Array of descriptors, each containing the pin count, dirty status, etc\
	. */
    protected FrameDesc[] frametab;

    /** Maps current page numbers to frames; used for efficient lookups. */
    protected HashMap<Integer, FrameDesc> pagemap;

    /** The replacement policy to use. */
    protected Replacer replacer;
    
//-------------------------------------------------------------
    /** 
        you may add HERE variables NEEDED for calculating hit ratios 
        a public void printBhrAndRefCount() has been provided at the bottom 
        which is called from test modules. To use that
            either use the same variable names OR
            modify the print method with variables you have used
    */     
//-------------------------------------------------------------
    public int totPageHits = 0;  // to calculate and store the total page hits
    public int totPageRequests = 0;  // to calculate and store the total page loads
    public ArrayList<ArrayList<Integer>> page_ref = new ArrayList();  // array list to store the page reference counts
    public ArrayList<Integer> pid = new ArrayList();  // sub array list to store pageno.pid
    public ArrayList<Integer> pagehit = new ArrayList();  // sub array list to store hit count of each pageno.pid
    public ArrayList<Integer> pagecount = new ArrayList();  // sub array list to store load count of each pageno.pid
    public int pageFaults = 0;  // to store the total number of page faults
    public float aggregateBHR = 0;  // to calculate and store the BHR value
    
    
  /**
   * Constructs a buffer mamanger with the given settings.
   * 
   * @param numbufs number of buffers in the buffer pool
   */
  public BufMgr(int numbufs) 
  {   
	  //initializing buffer pool and frame table 
      numbufs = NUMBUF;
      bufpool = new Page[numbufs];
      frametab = new FrameDesc[numbufs];
      
      for(int i = 0; i < frametab.length; i++)
      {
              bufpool[i] = new Page();
    	  	  frametab[i] = new FrameDesc(i);
      }
      
      //initializing page map and replacer here. 
      pagemap = new HashMap<Integer, FrameDesc>(numbufs);
      System.out.println("Replacement policy: LRU\nBuffer size: " + numbufs);
	  replacer = new LRU(this);   // change Policy to replacement class name -- done |Pratik 09/12|
      page_ref.add(pid);
      page_ref.add(pagehit);
      page_ref.add(pagecount);
  }

  /**
   * Allocates a set of new pages, and pins the first one in an appropriate
   * frame in the buffer pool.
   * 
   * @param firstpg holds the contents of the first page
   * @param run_size number of pages to allocate
   * @return page id of the first new page
   * @throws IllegalArgumentException if PIN_MEMCPY and the page is pinned
   * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)
   */

  public PageId newPage(Page firstpg, int run_size)
  {
	  //Allocating set of new pages on disk using run size.
	  PageId firstpgid = Minibase.DiskManager.allocate_page(run_size);
	  try {
		  //pin the first page using pinpage() function using the id of firstpage, page firstpg and skipread = PIN_MEMCPY(true)
		  pinPage(firstpgid, firstpg, PIN_MEMCPY);
          }
          catch (Exception e) {
        	  //pinning failed so deallocating the pages from disk
        	  for(int i=0; i < run_size; i++)
        	  {   
        		  firstpgid.pid += i;
 	  	          Minibase.DiskManager.deallocate_page(firstpgid);
        	  }
        	  return null;
      }
	  
	  //notifying replacer
      replacer.newPage(pagemap.get(Integer.valueOf(firstpgid.pid)));
      
      // you may have to add some BHR code here
      // Edit: No initialization code added here as we have used ArrayList to store 
      // the Pageno.pid and their corresponding hit counts and load counts
      return firstpgid; 
  }
  
  /**
   * Deallocates a single page from disk, freeing it from the pool if needed.
   * 
   * @param pageno identifies the page to remove
   * @throws IllegalArgumentException if the page is pinned
   */
  public void freePage(PageId pageno) 
  {  
	  //the frame descriptor as the page is in the buffer pool 
	  FrameDesc tempfd = pagemap.get(Integer.valueOf(pageno.pid));
	  //the page is in the pool so it cannot be null.
      if(tempfd != null)
      {
    	  //checking the pin count of frame descriptor
          if(tempfd.pincnt > 0)
              throw new IllegalArgumentException("Page currently pinned");
          //remove page as it's pin count is 0, remove the page, updating its pin count and dirty status, the policy and notifying replacer.
          pagemap.remove(Integer.valueOf(pageno.pid));
          tempfd.pageno.pid = INVALID_PAGEID;
          tempfd.pincnt = 0;
          tempfd.dirty = false;
          tempfd.state = Policy.AVAILABLE;
          replacer.freePage(tempfd);
      }
      //deallocate the page from disk 
      Minibase.DiskManager.deallocate_page(pageno);
  }

  /**
   * Pins a disk page into the buffer pool. If the page is already pinned, this
   * simply increments the pin count. Otherwise, this selects another page in
   * the pool to replace, flushing it to disk if dirty.
   * 
   * @param pageno identifies the page to pin
   * @param page holds contents of the page, either an input or output param
   * @param skipRead PIN_MEMCPY (replace in pool); PIN_DISKIO (read the page in)
   * @throws IllegalArgumentException if PIN_MEMCPY and the page is pinned
   * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)
   */
  public void pinPage(PageId pageno, Page page, boolean skipRead) 
  {  
	  //the frame descriptor as the page is in the buffer pool 
	  int ind;
      int prev_count;
	  FrameDesc tempfd = pagemap.get(Integer.valueOf(pageno.pid));
	  if(tempfd != null)
	  {
		  //if the page is in the pool and already pinned then by using PIN_MEMCPY(true) throws an exception "Page pinned PIN_MEMCPY not allowed" 
          if(skipRead)
        	  throw new IllegalArgumentException("Page pinned so PIN_MEMCPY not allowed");
          else
          {
        	  //else the page is in the pool and has not been pinned so incrementing the pincount and setting Policy status to pinned
        	  tempfd.pincnt++;
        	  tempfd.state = Policy.PINNED;
              page.setPage(bufpool[tempfd.index]);
              //some BHR code may go here
              // This section is executed when the page is hit. Therefore, incrementing the page hit count
              // corresponding to the pageno.pid
              // code added by Pratik on 09/18
            if (pageno.pid > 8){   // checking if the pageno.pid is greater than 8
              // check if the pageno.pid is found in the page_ref arraylist
              if (page_ref.get(0).indexOf(pageno.pid) == -1){  // -1 indicates that it is not found in the arraylist
                // if not found then add the pageno.pid in the page_ref array list and set the hit count to 1
                // and load count to 0
                page_ref.get(0).add(pageno.pid);
                page_ref.get(1).add(1);
                page_ref.get(2).add(0);
              }
              else{ 
                // if the pageno.pid is found in the page_ref array list then
                // just increment the hit count of the corresponding pageno.pid in the page_ref list
                ind = page_ref.get(0).indexOf(pageno.pid);
                prev_count = page_ref.get(1).get(ind);
                page_ref.get(1).set(ind, prev_count + 1);
              }
              // code added by Pratik 09/18 ends here
            }
        
              return;
          }
	  }
	  else
	  {
		  //as the page is not in pool choosing a victim
          int i = replacer.pickVictim();
          //if buffer pool is full throws an Exception("Buffer pool exceeded")
          if(i < 0)
        	  throw new IllegalStateException("Buffer pool exceeded");
                
          tempfd = frametab[i];
        // Check code for printing the number of times the page being replaced is referenced.
        //   int ind1 = page_ref.get(0).indexOf(tempfd.pageno.pid);
        //   if (ind1 > -1){
        //     int ref_count = page_ref.get(1).get(ind1) + page_ref.get(2).get(ind1);
        //     if (ref_count > 0){
        //         System.out.println("Page no. (PID): " + tempfd.pageno.pid + " has been referenced " + ref_count + " times.");
        //   }
        //   }

          //if the victim is dirty writing it to disk 
          if(tempfd.pageno.pid != -1)
          {
        	  pagemap.remove(Integer.valueOf(tempfd.pageno.pid));
        	  if(tempfd.dirty)
           		  Minibase.DiskManager.write_page(tempfd.pageno, bufpool[i]);
// some BHR code may go here
            // BHR code modification starts here
            // if the page is dirty then set the page reference counts to 0
            if (pageno.pid > 8){ 
                if (page_ref.get(0).indexOf(pageno.pid) != -1){
                    ind = page_ref.get(0).indexOf(pageno.pid);
                    page_ref.get(1).set(ind, 0);
                    page_ref.get(2).set(ind, 0);
                }
            }
            // BHR code modification ends here
          }
          //reading the page from disk to the page given and pinning it. 
          if(skipRead)
        	  bufpool[i].copyPage(page);
          else
          	  Minibase.DiskManager.read_page(pageno, bufpool[i]);
          
          page.setPage(bufpool[i]);
// some BHR code may go here
          // page fault variable is incremented here as the page requested is not found in the buffer
          pageFaults = pageFaults + 1;

          // only increment load page count here
          // code added by Pratik on 09/18
        // if pageno.pid is greater than 8 then
        if (pageno.pid > 8){ 
          // check if the pid is found in page_ref list
          // if not found then add it to the list and set the load count to 1 and hit count to 0
          if (page_ref.get(0).indexOf(pageno.pid) == -1){
                page_ref.get(0).add(pageno.pid);
                page_ref.get(1).add(0);
                page_ref.get(2).add(1);
              }
          else{
            // if pid found in the page_ref list then increment the load count
                ind = page_ref.get(0).indexOf(pageno.pid);
                prev_count = page_ref.get(2).get(ind);
                page_ref.get(2).set(ind, prev_count + 1);
              }
        }
      // code added by Pratik 09/18 ends here

	  }
	  	  //updating frame descriptor and notifying to replacer
	      tempfd.pageno.pid = pageno.pid;
          tempfd.pincnt = 1;
          tempfd.dirty = false;
          pagemap.put(Integer.valueOf(pageno.pid), tempfd);
          tempfd.state =Policy.PINNED;
      	  replacer.pinPage(tempfd);
   
  }

  /**
   * Unpins a disk page from the buffer pool, decreasing its pin count.
   * 
   * @param pageno identifies the page to unpin
   * @param dirty UNPIN_DIRTY if the page was modified, UNPIN_CLEAN otherrwise
   * @throws IllegalArgumentException if the page is not present or not pinned
   */
  public void unpinPage(PageId pageno, boolean dirty) 
  {  
	  //the frame descriptor as the page is in the buffer pool 
	  FrameDesc tempfd = pagemap.get(Integer.valueOf(pageno.pid));
	  
	  //if page is not present an exception is thrown as "Page not present"
      if(tempfd == null)
          throw new IllegalArgumentException("Page not present");
      
      //if the page is present but not pinned an exception is thrown as "page not pinned"
      if(tempfd.pincnt == 0)
      {
          throw new IllegalArgumentException("Page not pinned");
      } 
      else
      {
    	  //unpinning the page by decrementing pincount and updating the frame descriptor and notifying replacer
          tempfd.pincnt--;
          tempfd.dirty = dirty;
          if(tempfd.pincnt== 0)
          tempfd.state = Policy.REFERENCED;
          replacer.unpinPage(tempfd);
          return;
      }
  }

  /**
   * Immediately writes a page in the buffer pool to disk, if dirty.
   */
  public void flushPage(PageId pageno) 
  {  
	  for(int i = 0; i < frametab.length; i++)
	 	  //checking for pageid or id the pageid is the frame descriptor and the dirty status of the page
          if((pageno == null || frametab[i].pageno.pid == pageno.pid) && frametab[i].dirty)
          {
        	  //writing down to disk if dirty status is true and updating dirty status of page to clean
              Minibase.DiskManager.write_page(frametab[i].pageno, bufpool[i]);
              frametab[i].dirty = false;
          }
  }

  /**
   * Immediately writes all dirty pages in the buffer pool to disk.
   */
  public void flushAllPages() 
  {
	  for(int i=0; i<frametab.length; i++) 
		  flushPage(frametab[i].pageno);
  }

  /**
   * Gets the total number of buffer frames.
   */
  public int getNumBuffers() 
  {
	  return bufpool.length;
  }

  /**
   * Gets the total number of unpinned buffer frames.
   */
  public int getNumUnpinned() 
  {
	  int numUnpinned = 0;
	  for(int i=0; i<frametab.length; i++) 
	  {
		  if(frametab[i].pincnt == 0)
			  numUnpinned++;
	  }
	  return numUnpinned;
  }
  
    // Function to sort the page references list and print
    // Reference: Stack overflow post by apoori user account
    // Link: https://stackoverflow.com/questions/34745203/using-a-for-loop-to-manually-sort-an-array-java
    // Modified the code for sorting the array list and get the sorted indexes
    public void sort_func(){
        int[] sorted_indexes = new int[page_ref.get(0).size()];
        for (int i = 0; i < sorted_indexes.length; i++){
            sorted_indexes[i] = i;
        }

        for(int i = 0 ; i < sorted_indexes.length;i++)
        {
            for(int j = i+1 ; j < sorted_indexes.length;j++)
            {
                if(page_ref.get(1).get(sorted_indexes[i]) < page_ref.get(1).get(sorted_indexes[j]))
                {
                    int temp = sorted_indexes[i];
                    sorted_indexes[i] = sorted_indexes[j];
                    sorted_indexes[j] = temp;
                }
            }
        }

        System.out.println("Page no.\tNo. of Loads\tNo. of Page Hits");
        for (int i: sorted_indexes){
            System.out.println(page_ref.get(0).get(i) + "\t\t" + page_ref.get(2).get(i) + "\t\t" + page_ref.get(1).get(i));
        }
        System.out.println("+----------------------------------------+\n\n");
    }

    // Function printing the BHR and ref counts
    public void printBhrAndRefCount(){ 
    
    // Calculate the total page hits
    ArrayList<Integer> temp = page_ref.get(1);
    for (int i = 0; i < temp.size(); i++){
        totPageHits = temp.get(i) + totPageHits;
    }
    
    // Calculating the total page loads
    temp = page_ref.get(2);
    for (int i = 0; i < temp.size(); i++){
        totPageRequests = temp.get(i) + totPageRequests;
    }
    
    System.out.println("+----------------------------------------+");
    System.out.println("Aggregate Page Hits: " +totPageHits);
    System.out.println("+----------------------------------------+\n");
    System.out.println("Aggregate Page Loads: " +totPageRequests);
    System.out.println("+----------------------------------------+\n");
    
    //compute BHR1 and BHR2 
    if (totPageRequests != 0){
        aggregateBHR = (float) totPageHits/totPageRequests; //replce -1 with the formula
        System.out.println("BHR (for the whole buffer): " +aggregateBHR);
    }
    else{
        System.out.println("Divide by zero error. totPageRequests value is zero while calculating BHR."); 
    }
    
    System.out.println("+----------------------------------------+\n");
    System.out.println("Page Faults: " +pageFaults);
    System.out.println("+----------------------------------------+\n");
    System.out.println("The top k (10) referenced pages are:");
    System.out.println("+----------------------------------------+\n");
    sort_func();
    }

} // public class BufMgr implements GlobalConst
