package map;

public class Backend {

	public static void main(String[] args) {
		FirstRunning firstRun = new FirstRunning();
		try {
			firstRun.runFirstMap(args[0]);
		} catch (Exception e) {
			e.printStackTrace();
		}
		SecondRunning secondRun = new SecondRunning();
		try {
			secondRun.runSecondMap();
		} catch (Exception e) {
			e.printStackTrace();
		}
		/*
		 * DON'T FORGET TO SET THE PATHS; i.e.: First run gets args[0].
		 * Second run is pathed the way I want to path.
		 * Third run is given the output of args[1].
		 */
		

	}

}
