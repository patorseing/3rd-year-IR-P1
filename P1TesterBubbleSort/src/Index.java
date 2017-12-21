/*member
 * 1. Miss Nutthariga Sulsaksakul 5888177 sec 2
 * 2. Miss Kanjanaporn Sumitdech 5888178 sec 2
 * 3. Miss Napatchol Thaipanich 5888205 sec 2
 */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class Index {

	// Term id -> (position in index file, doc frequency) dictionary
	private static Map<Integer, Pair<Long, Integer>> postingDict 
		= new TreeMap<Integer, Pair<Long, Integer>>();
	// Doc name -> doc id dictionary
	private static Map<String, Integer> docDict
		= new TreeMap<String, Integer>();
	// Term -> term id dictionary
	private static Map<String, Integer> termDict
		= new TreeMap<String, Integer>();
	// Block queue
	private static LinkedList<File> blockQueue
		= new LinkedList<File>();

	// Total file counter
	private static int totalFileCount = 0;
	// Document counter
	private static int Doc_IdCounter = 0;
	// Term counter
	private static int wordIdCounter = 0;
	// Index
	private static BaseIndex index = null;

	
	/* 
	 * Write a posting list to the given file 
	 * You should record the file position of this posting list
	 * so that you can read it back during retrieval
	 * 
	 * */
	private static void writePosting(FileChannel fc, PostingList posting)
			throws IOException {
		/*
		 * #TODO: Your code here
		 *	 
		 */
		if(blockQueue.isEmpty()) {
			postingDict.put(posting.getTermId(),new Pair<Long,Integer>(fc.position(),posting.getList().size()));
		}
		index.writePosting(fc, posting);
	}
	

	 /**
     * Pop next element if there is one, otherwise return null
     * @param iter an iterator that contains integers
     * @return next element or null
     */
    private static Integer popNextOrNull(Iterator<Integer> iter) {
        if (iter.hasNext()) {
            return iter.next();
        } else {
            return null;
        }
    }
	
	/**
	 * Main method to start the indexing process.
	 * @param method		:Indexing method. "Basic" by default, but extra credit will be given for those
	 * 			who can implement variable byte (VB) or Gamma index compression algorithm
	 * @param dataDirname	:relative path to the dataset root directory. E.g. "./datasets/small"
	 * @param outputDirname	:relative path to the output directory to store index. You must not assume
	 * 			that this directory exist. If it does, you must clear out the content before indexing.
	 */
	public static int runIndexer(String method, String dataDirname, String outputDirname) throws IOException 
	{
		/* Get index */
		String className = method + "Index";
		try {
			Class<?> indexClass = Class.forName(className);
			index = (BaseIndex) indexClass.newInstance();
		} catch (Exception e) {
			System.err
					.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}
		
		/* Get root directory */
		File rootdir = new File(dataDirname);
		if (!rootdir.exists() || !rootdir.isDirectory()) {
			System.err.println("Invalid data directory: " + dataDirname);
			return -1;
		}
		
		   
		/* Get output directory*/
		File outdir = new File(outputDirname);
		if (outdir.exists() && !outdir.isDirectory()) {
			System.err.println("Invalid output directory: " + outputDirname);
			return -1;
		}
		
		/*	#TODO: delete all the files/sub folder under outdir
		 * 
		 */
		File[] List = outdir.listFiles();	// List is list of file in the folder.
		if(outdir.exists()&&outdir.isDirectory()) {
			for (File file : List){	
				file.delete();
			}
		}
		
		
		if (!outdir.exists()) {
			if (!outdir.mkdirs()) {
				System.err.println("Create output directory failure");
				return -1;
			}
		}
		
		/* BSBI indexing algorithm */
		File[] dirlist = rootdir.listFiles();
		
		/* For each block */
		for (File block : dirlist) {
			if(block.getName().equals(".DS_Store")) {
				continue;
			}
			File blockFile = new File(outputDirname, block.getName());
			//System.out.println("Processing block "+block.getName());
			blockQueue.add(blockFile);
			
			//System.out.println("=" + dataDirname + "-" + block.getName());

			File blockDir = new File(dataDirname, block.getName());
			File[] filelist = blockDir.listFiles();
			
			
			TreeMap<Integer, TreeSet<Integer> > Map = new TreeMap<Integer, TreeSet<Integer>>();
			
			/* For each file */
			for (File file : filelist) {
				++totalFileCount;
				String fileName = block.getName() + "/" + file.getName();
				
				 // use pre-increment to ensure Doc_Id > 0
                int Doc_Id = ++Doc_IdCounter;
                docDict.put(fileName, Doc_Id);
			
				BufferedReader reader = new BufferedReader(new FileReader(file));
				String line;
				while ((line = reader.readLine()) != null) {
					String[] tokens = line.trim().split("\\s+");
					for (String token : tokens) {
						/*
						 * #TODO: Your code here
						 *       For each term, build up a list of
						 *       documents in which the term occurs
						 */
						if (!termDict.containsKey(token)){
							termDict.put(token, wordIdCounter++);
						}
						//Check if term is in blockMap, add if not
						if (!Map.containsKey(termDict.get(token))){
							Map.put(termDict.get(token), new TreeSet<Integer>());
						}
						//Add the docId to the token's docSet
						Map.get(termDict.get(token)).add(docDict.get(fileName));
					}
				}
				reader.close();
			}

			/* Sort and output */
			if (!blockFile.createNewFile()) {
				System.err.println("Create new block failure.");
				return -1;
			}
			
			RandomAccessFile Bfc = new RandomAccessFile(blockFile, "rw");
			
			/*
			 * #TODO: Your code here
			 *       Write all posting lists for all terms to file (Bfc) 
			 */
			FileChannel fc = Bfc.getChannel();
			for (int termId : Map.keySet()){
				index.writePosting(fc, new PostingList(termId,new ArrayList<Integer>(Map.get(termId))));
			}
			Bfc.close();
		}

		/* Required: output total number of files. */
		//System.out.println("Total Files Indexed: "+totalFileCount);

		/* Merge blocks */
		while (true) {
			if (blockQueue.size() <= 1)
				break;

			File b1 = blockQueue.removeFirst();
			File b2 = blockQueue.removeFirst();
			
			File combfile = new File(outputDirname, b1.getName() + "+" + b2.getName());
			if (!combfile.createNewFile()) {
				System.err.println("Create new block failure.");
				return -1;
			}

			RandomAccessFile bf1 = new RandomAccessFile(b1, "r");
			RandomAccessFile bf2 = new RandomAccessFile(b2, "r");
			RandomAccessFile mf = new RandomAccessFile(combfile, "rw");
			 
			/*
			 * #TODO: Your code here
			 *       Combine blocks bf1 and bf2 into our combined file, mf
			 *       You will want to consider in what order to merge
			 *       the two blocks (based on term ID, perhaps?).
			 *       
			 */
			FileChannel Bfc1 = bf1.getChannel();
			FileChannel Bfc2 = bf2.getChannel();
			FileChannel mfc = mf.getChannel();
			
			PostingList post1 = index.readPosting(Bfc1);
			PostingList post2 = index.readPosting(Bfc2);
			PostingList post3 = post1,post4 = post2;
			do {
                post1 = index.readPosting(Bfc1);
                post2 = index.readPosting(Bfc2);
                while (post1 != null && (post2 == null || post1.getTermId() < post2.getTermId())) {
                    writePosting(mfc, post1);
                    post1 = index.readPosting(Bfc1);
                }
                while (post2 != null && (post1 == null || post2.getTermId() < post1.getTermId())) {
                    writePosting(mfc, post2);
                    post2 = index.readPosting(Bfc2);
                }
                if (post1 != null && post2 != null && post1.getTermId() == post2.getTermId()) {
                	   PostingList list = mergePostings(post1, post2);
                    writePosting(mfc, list);
                }
            } while (post1 != null || post2 != null);
			
			bf1.close();
			bf2.close();
			mf.close();
			b1.delete();
			b2.delete();
			blockQueue.add(combfile);
		}

		/* Dump constructed index back into file system */
		File indexFile = blockQueue.removeFirst();
		indexFile.renameTo(new File(outputDirname, "corpus.index"));

		BufferedWriter termWriter = new BufferedWriter(new FileWriter(new File(
				outputDirname, "term.dict")));
		for (String term : termDict.keySet()) {
			termWriter.write(term + "\t" + termDict.get(term) + "\n");
		}
		termWriter.close();

		BufferedWriter docWriter = new BufferedWriter(new FileWriter(new File(
				outputDirname, "doc.dict")));
		for (String doc : docDict.keySet()) {
			docWriter.write(doc + "\t" + docDict.get(doc) + "\n");
		}
		docWriter.close();

		BufferedWriter postWriter = new BufferedWriter(new FileWriter(new File(
				outputDirname, "posting.dict")));
		for (Integer termId : postingDict.keySet()) {
			postWriter.write(termId + "\t" + postingDict.get(termId).getFirst()
					+ "\t" + postingDict.get(termId).getSecond() + "\n");
		}
		postWriter.close();
		
		return totalFileCount;
	}
	
	private static PostingList mergePostings(PostingList post1, PostingList post2) {
        
		if (post1.getList().isEmpty()) return post2;
        if (post2.getList().isEmpty()) return post1;
        
		Iterator<Integer> Iterator1 = post1.getList().iterator();
        Iterator<Integer> Iterator2 = post2.getList().iterator();
        
        PostingList posting_list = new PostingList(post1.getTermId());
        
        Integer doc_Id1 = popNextOrNull(Iterator1);
        Integer doc_Id2 = popNextOrNull(Iterator2);
        
        while (doc_Id1 != null && doc_Id2 != null) {
            if (doc_Id1.equals(doc_Id2)) {
            		posting_list.getList().add(doc_Id1);
                doc_Id1 = popNextOrNull(Iterator1);
                doc_Id2 = popNextOrNull(Iterator2);
            } else if (doc_Id1 < doc_Id2) {
            		posting_list.getList().add(doc_Id1);
                doc_Id1 = popNextOrNull(Iterator1);
            } else {
            		posting_list.getList().add(doc_Id2);
                doc_Id2 = popNextOrNull(Iterator2);
            }
        }
        while (doc_Id1 != null) {
        		posting_list.getList().add(doc_Id1);
            doc_Id1 = popNextOrNull(Iterator1);
        }
        while (doc_Id2 != null) {
        		posting_list.getList().add(doc_Id2);
            doc_Id2 = popNextOrNull(Iterator2);
        }

        return posting_list;
    }

	public static void main(String[] args) throws IOException {
		/* Parse command line */
		if (args.length != 3) {
			System.err
					.println("Usage: java Index [Basic|VB|Gamma] data_dir output_dir");
			return;
		}

		/* Get index */
		String className = "";
		try {
			className = args[0];
		} catch (Exception e) {
			System.err
					.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}

		/* Get root directory */
		String root = args[1];
		

		/* Get output directory */
		String output = args[2];
		runIndexer(className, root, output);
	}

}
