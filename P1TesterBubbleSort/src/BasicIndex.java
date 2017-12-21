/*member
 * 1. Miss Nutthariga Sulsaksakul 5888177 sec 2
 * 2. Miss Kanjanaporn Sumitdech 5888178 sec 2
 * 3. Miss Napatchol Thaipanich 5888205 sec 2
 */

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.LinkedList;

public class BasicIndex implements BaseIndex {

	@Override
	public PostingList readPosting(FileChannel fc) {
		/*
		 * #TODO: Your code here
		 *       Read and return the postings list from the given file.
		 */
		ByteBuffer buf = ByteBuffer.allocate(8);	// buf stand for buffer, that reserved 8 bytes
		
		try {
			
			if(fc.read(buf) == -1) {	//if there is no data come into buffer, it will return null;
				return null;
			}
			buf.flip();	// flip to set the limit to position
			
			// define Document number termID, docFreq 
			int termID = buf.getInt();
			int docFreq = buf.getInt();
			buf = ByteBuffer.allocate(4*docFreq);
			
			fc.read(buf);
			buf.flip();	// flip to set the limit to position
			
			ArrayList<Integer> docIDs = new ArrayList<>();
			
			for(int i = 0; i<docFreq;i++) {
				docIDs.add(buf.getInt());
			}
	        
			PostingList postings_list = new PostingList(termID, docIDs); // sent the data to posting list class
	        
	        
	        return postings_list;
		} catch (IOException e) {
			// #TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public void writePosting(FileChannel fc, PostingList p) {
		/*
		 * #TODO: Your code here
		 *       Write the given postings list to the given file.
		 */
		try {
			int termID = p.getTermId();
			int docFreq = p.getList().size();
		
			ByteBuffer buf = ByteBuffer.allocate(4*(docFreq+2));
			buf.putInt(termID);
			buf.putInt(docFreq);
		
			for(int docID : p.getList()) {
				buf.putInt(docID);
			}
			buf.flip();
			
			fc.write(buf);
		} catch (IOException e) {
			// #TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

