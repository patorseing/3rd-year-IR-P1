/*member
 * 1. Miss Nutthariga Sulsaksakul 5888177 sec 2
 * 2. Miss Kanjanaporn Sumitdech 5888178 sec 2
 * 3. Miss Napatchol Thaipanich 5888205 sec 2
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

public class Query {

	// Term id -> position in index file
	private  Map<Integer, Long> posDict = new TreeMap<Integer, Long>();
	// Term id -> document frequency
	private  Map<Integer, Integer> freqDict = new TreeMap<Integer, Integer>();
	// Doc id -> doc name dictionary
	private  Map<Integer, String> docDict = new TreeMap<Integer, String>();
	// Term -> term id dictionary
	private  Map<String, Integer> termDict = new TreeMap<String, Integer>();
	// Index
	private  BaseIndex index = null;
	

	//indicate whether the query service is running or not
	private boolean running = false;
	private RandomAccessFile indexFile = null;
	
	/* 
	 * Read a posting list with a given termID from the file 
	 * You should seek to the file position of this specific
	 * posting list and read it back.
	 * */
	private  PostingList readPosting(FileChannel fc, int termId)
			throws IOException {
		/*
		 * #TODO: Your code here
		 */
		if(!posDict.containsKey(termId)) {
			return null;
		}
		else {
			long pos = posDict.get(termId);
	        fc.position(pos);
	        return index.readPosting(fc);
		}
	}
	
	
	public void runQueryService(String indexMode, String indexDirname) throws IOException
	{
		//Get the index reader
		try {
			Class<?> indexClass = Class.forName(indexMode+"Index");
			index = (BaseIndex) indexClass.newInstance();
		} catch (Exception e) {
			System.err
					.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}
		
		//Get Index file
		File inputdir = new File(indexDirname);
		if (!inputdir.exists() || !inputdir.isDirectory()) {
			System.err.println("Invalid index directory: " + indexDirname);
			return;
		}
		
		/* Index file */
		indexFile = new RandomAccessFile(new File(indexDirname,"corpus.index"), "r");

		String line = null;
		/* Term dictionary */
		BufferedReader termReader = new BufferedReader(new FileReader(new File(
				indexDirname, "term.dict")));
		while ((line = termReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			termDict.put(tokens[0], Integer.parseInt(tokens[1]));
		}
		termReader.close();

		/* Doc dictionary */
		BufferedReader docReader = new BufferedReader(new FileReader(new File(
				indexDirname, "doc.dict")));
		while ((line = docReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			docDict.put(Integer.parseInt(tokens[1]), tokens[0]);
		}
		docReader.close();

		/* Posting dictionary */
		BufferedReader postReader = new BufferedReader(new FileReader(new File(
				indexDirname, "posting.dict")));
		while ((line = postReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			posDict.put(Integer.parseInt(tokens[0]), Long.parseLong(tokens[1]));
			freqDict.put(Integer.parseInt(tokens[0]),
					Integer.parseInt(tokens[2]));
		}
		postReader.close();
		
		this.running = true;
	}
    
	public List<Integer> retrieve(String query) throws IOException
	{	if(!running) 
		{
			System.err.println("Error: Query service must be initiated");
		}
		
		/*
		 * TODO: Your code here
		 *       Perform query processing with the inverted index.
		 *       return the list of IDs of the documents that match the query
		 *      
		 */
		String querys[] = query.trim().split("\\s+");
		
		List<Integer> result = new ArrayList<Integer>();
		int size = 0;
		size = querys.length;
		int terms[] = new int[size];
		
		for (int i = 0; i < size; i++) {
			Integer temp = termDict.get(querys[i]);
			if (temp == null) {
				return result;
			}
			terms[i] = temp;
		}
		
		//Quick Sort
		quickSort(terms);
		
		FileChannel fc = indexFile.getChannel();
		result = readPosting(fc, terms[0]).getList();
		List<Integer> re;
		
		for(int i=1;i<terms.length;i++) {
			re=readPosting(fc, terms[i]).getList();
			for(int j=0;j<result.size();j++) {
				if(!re.contains(result.get(j))){
					result.remove(j);
					j--;
				}
			}
		}
        return result;
	}
	
	public static void quickSort(int a[]){
		int nElems = a.length;
		recQuickSort(0, nElems-1, a);
	}
	
	private static void recQuickSort(int left, int right, int b[]) {
		int size = right - left + 1;
	    if (size <= 3)
	    	Sort(left, right, b);
	    else {
	      int median = findpivot(left, right, b);
	      int partition = partition(left, right, median,b);
	      recQuickSort(left, partition - 1,b);
	      recQuickSort(partition + 1, right,b);
	    }
	}
	
	public static void Sort(int left, int right,int[] e) {
	    int size = right - left + 1;
	    if (size <= 1)
	      return;
	    if (size == 2) {
	      if (e[left] > e[right])
	        swap(right, left, e);
	      return;
	    } else {
	      if (e[left] > e[right - 1])
	        swap(right - 1, left, e);
	      if (e[left] > e[right])
	        swap(right, left, e);
	      if (e[right - 1] > e[right])
	        swap(right, right - 1, e);
	    }
	  }
	
	private static int partition(int left, int right, double median, int[] b) {
		int leftPtr = left;
	    int rightPtr = right - 1;

	    while (true) {
	      while (b[++leftPtr] < median)
	        ;
	      while (b[--rightPtr] > median)
	        ;
	      if (leftPtr >= rightPtr)
	        break;
	      else
	        swap(rightPtr, leftPtr, b);
	    }
	    swap(right - 1, leftPtr, b);
	    return leftPtr;
	}
	
	private static void swap(int dex1, int dex2, int[] d) {
		int temp = d[dex1];
	    d[dex1] = d[dex2];
	    d[dex2] = temp;
	}
	
	private static int findpivot(int left, int right,int[] c) {	 //receive position in left way and right way
		int mid = (left+right)/2;								 //computing center position
		// compare and call swap method for find pivot in center at the last
		if (c[left] > c[mid]){
			swap(left, mid,c);
		}
		if (c[left] > c[right]){
			swap(left, right,c);
		}
		if (c[mid] > c[right]){
			swap(mid, right,c);
		}

		swap(mid, right - 1,c);
		// sent pivot back for using
		return c[right - 1];
	}
	
    String outputQueryResult(List<Integer> res) {
        /*
         * #TODO: 
         * 
         * Take the list of documents ID and prepare the search results, sorted by lexicon order. 
         * 
         * E.g.
         * 	0/fine.txt
		 *	0/hello.txt
		 *	1/bye.txt
		 *	2/fine.txt
		 *	2/hello.txt
		 *
		 * If there no matched document, output:
		 * 
		 * no results found
		 * 
         * */
    		String out = "";
    		if(res.isEmpty()) {
    			return "no results found";
    		}
    		else {
    			for(int id: res) {
    				out+= docDict.get(id)+"\n";
    			}
    		}
    		
    		return out;
    }
	
	public static void main(String[] args) throws IOException {
		/* Parse command line */
		if (args.length != 2) {
			System.err.println("Usage: java Query [Basic|VB|Gamma] index_dir");
			return;
		}

		/* Get index */
		String className = null;
		try {
			className = args[0];
		} catch (Exception e) {
			System.err
					.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}

		/* Get index directory */
		String input = args[1];
		
		Query queryService = new Query();
		queryService.runQueryService(className, input);
		
		/* Processing queries */
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		/* For each query */
		String line = null;
		while ((line = br.readLine()) != null) {
			List<Integer> hitDocs = queryService.retrieve(line);
			queryService.outputQueryResult(hitDocs);
		}
		
		br.close();
	}
	
	protected void finalize()
	{
		try {
			if(indexFile != null)indexFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

