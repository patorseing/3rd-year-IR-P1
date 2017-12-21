
public class QuickSortApp {

	public static void main(String[] args) {
		int maxSize = 40;
		int[] data = new int[maxSize];
		for(int j=0; j<maxSize; j++){
			data[j] = (int) (java.lang.Math.random()*99);
			System.out.print(data[j]+" ");
		}
		System.out.println();
		quickSort(data);
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
		System.out.println("pivot : "+c[right - 1]);
		// sent pivot back for using
		return c[right - 1];
	}

	private static void swap(int dex1, int dex2, int[] d) {
		int temp = d[dex1];
	    d[dex1] = d[dex2];
	    d[dex2] = temp;
	}
	
	public static void quickSort(int a[]){
		int nElems = a.length;
		recQuickSort(0, nElems-1, a);
		for(int j=0; j<nElems; j++){
			System.out.print(a[j]+" ");
		}
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
}
