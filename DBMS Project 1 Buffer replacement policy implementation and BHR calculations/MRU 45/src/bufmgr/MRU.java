package bufmgr;

import diskmgr.*;
import global.*;
import java.io.*;
import java.util.Arrays;

  /**
   * class Policy is a subclass of class Replacer use the given replacement
   * policy algorithm for page replacement
   */
class MRU extends Replacer {   
//replace Policy above with impemented policy name (e.g., Lru, Clock) -- done |Pratik 09/12|
  //
  // Frame State Constants
  //
  protected static final int AVAILABLE = 10;
  protected static final int REFERENCED = 11;
  protected static final int PINNED = 12;

  //Following are the fields required for LRU and MRU policies:
  /**
   * private field
   * An array to hold number of frames in the buffer pool
   */

  private int frames[];
 
  /**
   * private field
   * number of frames used
   */   
  private int  nframes;

  /** Clock head; required for the default clock algorithm. */
  protected int head;

  /**
   * This pushes the given frame to the end of the list.
   * @param frameNo	the frame number
   */
  private void update(int frameNo)
  {
    // written by Pratik on 09/15
    // MRU replacement policy: For MRU we are treating the frames array as Stack
    // and the item in frames array with the index frameNo contains the index of the most recently used
    // so move that item to the start of the frames array and shift all the other items in the array
    // to the right
    int temp = frames[frameNo]; // temp variable to hold the index at frameNo position in frames array

    // Shifting and moving operations performed below using for loop
    for (int i = frameNo - 1; i >= 0; i--){
      frames[i + 1] = frames[i];
    }
    frames[0] = temp;

    // Update function completed, nothing to return
  }

  /**
   * Class constructor
   * Initializing frames[] pinter = null.
   */
    public MRU(BufMgr mgrArg)
    {
      super(mgrArg);
      // initialize the frame states
    for (int i = 0; i < frametab.length; i++) {
      frametab[i].state = AVAILABLE;
    }
      // initialize parameters for LRU and MRU
      nframes = frametab.length - 1;   // initialize at the last index
      frames = new int[frametab.length];

      // edited by Pratik on 09/15
      // Initialize the frames array with -1 to indicate that the frame is not yet assigned a page
      for (int i = 0; i < frames.length; i++){
        frames[i] = -1;   // -1 to indicate that the frame is not yet assigned
      }

    // initialize the clock head for Clock policy
    head = -1;
    }
  /**
   * Notifies the replacer of a new page.
   */
  public void newPage(FrameDesc fdesc) {
    // no need to update frame state
  }

  /**
   * Notifies the replacer of a free page.
   */
  public void freePage(FrameDesc fdesc) {
    fdesc.state = AVAILABLE;
  }

  /**
   * Notifies the replacer of a pined page.
   */
  public void pinPage(FrameDesc fdesc) {
        
  }

  /**
   * Notifies the replacer of an unpinned page.
   */
  public void unpinPage(FrameDesc fdesc) {

  }
  
  /**
   * Finding a free frame in the buffer pool
   * or choosing a page to replace using your policy
   *
   * @return 	return the frame number
   *		return -1 if failed
   */

  
 public int pickVictim()
 {
    // written by Pratik on 09/14

    // Note: Here the nframes variable is used as pointer to the maximum index of frame assigned
    // The nframes variable can be used only till the time when all the buffer places are occupied
    // After that we scan for the frame which is available and return the index

    // Compare the size of the buffer pool allocated yet and frametab.length
    // This check is only for the first n pickVictim calls where n is the buffer size
    if (nframes >= 0){
      // If the nframes is less than the buffer size then 
      // write the frame index in the frames array
      frames[nframes] = nframes;  // replace the -1 with the index of the frame in frames array
      // decrement the pointer to the next 
      nframes = nframes - 1;
      // Mark the selected frame as pinned
      frametab[nframes + 1].state = PINNED;
      // return the required index
      return (nframes + 1);
    }

    // Variable to store the actual index of frames array
    int ind = 0;

    // Iterate through the framestab as per the indexes in the frames array
    // The order of the indexes in the frames array depends upon the policy used
    // Here we are using the MRU replacement policy
    for (int i: frames){
      // Search for a frame that is not pinned
      if (frametab[i].state != PINNED){
        // If an unpinned frame is found then update the frames array as  per replacement policy
        update(ind);
        // Mark the selected frame as pinned
        frametab[i].state = PINNED;
        // And return the required index
        return frametab[i].index;
      }
      ind = ind + 1;  // increment the frames array index for above logic
    }

    return -1;   //if buffer is full and all pages are pinned then return -1
 }
 }
