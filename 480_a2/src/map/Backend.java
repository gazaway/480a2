package map;

public class Backend {

	/*
	 * This class is my driver for running the map/reduce job.
	 * This job consists of three different map/reduce run-
	 * threws. Each can be seen in their respective
	 * '*Running.java' files. 
	 */
	public static void main(String[] args) {
		//Creates and runs the first Map/Reduce job
		FirstRunning firstRun = new FirstRunning();
		
		//Value to be passed into the third run. Value is
		//returned from the first run. numDocs = the
		//number of documents in the args[0] path.
		int numDocs = 0;
		try {
			numDocs = firstRun.runFirstMap(args[0]);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		//Creates and runs the second Map/Reduce job
		SecondRunning secondRun = new SecondRunning();
		try {
			secondRun.runSecondMap();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		//Creates and runs the third Map/Reduce job. Sets
		//the number of documents as well.
		ThirdRunning thirdRun = new ThirdRunning();
		boolean ok = thirdRun.setNumDocs(numDocs);
		if (ok){
			try {
			thirdRun.runThirdMap(args[1], numDocs);
			} catch (Exception e) {
			e.printStackTrace();
			}
		}
		else {
			for (int i = 0; i < 9; i++){
				System.out.println("~!*~!*~!*~!* ERROR GETTING NUMBER OF DOCS ~!*~!*~!*~!*");
			}
		}
	}
}
