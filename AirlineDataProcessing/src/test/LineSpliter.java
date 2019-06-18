package test;

public class LineSpliter {

	public static void main(String[] args) {
		String str = "1,Private flight,\\N,-,N/A,,,Y";
		String[] arr = str.split(",");
		
		
		System.out.print("length: "+ arr.length + "\nstuff: " + arr[arr.length - 1]);
		System.out.print("length: "+ arr.length + "\nstuff: " + arr[7].length());

	}

}
