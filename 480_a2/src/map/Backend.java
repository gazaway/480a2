package map;

public class Backend {

	public static void main(String[] args) {
		FirstMap firstRun = new FirstMap();
		try {
			firstRun.runFirstMap(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		

	}

}
