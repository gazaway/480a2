package map;

public class Backend {

	public static void main(String[] args) {
		//Creates and runs the first Map/Reduce job
		FirstRunning firstRun = new FirstRunning();
		
		//Value to be passed into the third run. Value is
		//returned from the first run. numDocs = the
		//number of documents in the args[0] path.
		int numDocs = -1;
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
		
		//Creates and runs the third Map/Reduce job
		ThirdRunning thirdRun = new ThirdRunning();
		try {
			thirdRun.runThirdMap(args[1], numDocs);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
