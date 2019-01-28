//importing required packages
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;
//Class that generates the outputs
public class Task3 {
	public static void main(String[] args) throws IOException{
		Scanner in = new Scanner(new FileReader("part-r-00000"));
		//Variable for Date on which maximum number of accidents took place.
	        String Date = new String();
		int DateMaxAccidents = Integer.MIN_VALUE;
		//Variable for Borough with maximum count of accident fatality
		String Borough  = new String();
		int BoroughMaxAccidents = Integer.MIN_VALUE;
		//Variable for Zip with maximum count of accident fatality
		String Zip  = new String();
		int ZipMaxAccidents = Integer.MIN_VALUE;
		//Variable for Which vehicle type is involved in maximum accidents
		String VehicleType = new String();
		int VehicleTypeMaxAccidents = Integer.MIN_VALUE;
		//Variable for Year in which maximum Number Of Persons and Pedestrians Injured
		String YearPpInured = new String();
		int YearPpInuredMaxAccidents = Integer.MIN_VALUE;
		//Variable for Year in which maximum Number Of Persons and Pedestrians Killed
		String YearPpKilled = new String();
		int YearPpKilledMaxAccidents = Integer.MIN_VALUE;
		//Variable for Year in which maximum Number Of Cyclist Injured and Killed (combined)
		String cyclist = new String();
		int cyclistMaxAccidents = Integer.MIN_VALUE;
		//Variable for Year in which maximum Number Of Motorist Injured and Killed (combined)
		String motorists = new String();
		int motoristsMaxAccidents = Integer.MIN_VALUE;
		//Calculating Date on which maximum number of accidents took place
		while(in.hasNext()) {
			String str = in.nextLine();
			if(str.equals("")) continue;
			if(str.startsWith("v1_")) {
				String temp = str.substring(3);
				char c = (char)(9);
				int index = temp.lastIndexOf(c);
				int index1 = temp.lastIndexOf(" ");
				index = Math.max(index, index1);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>DateMaxAccidents) {
					DateMaxAccidents = val;
					Date = first;
				}
			}
			//Calculating Borough with maximum count of accident fatality
			else if(str.startsWith("v2_")) {
				String temp = str.substring(3);
				char c = (char)(9);
				int index = temp.lastIndexOf(c);
				int index1 = temp.lastIndexOf(" ");
				index = Math.max(index, index1);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>BoroughMaxAccidents) {
					BoroughMaxAccidents = val;
					Borough = first;
				}
			}
			//Calculating Zip with maximum count of accident fatality
			else if(str.startsWith("v3_")) {
				String temp = str.substring(3);
				char c = (char)(9);
				int index = temp.lastIndexOf(c);
				int index1 = temp.lastIndexOf(" ");
				index = Math.max(index, index1);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>ZipMaxAccidents) {
					ZipMaxAccidents = val;
					Zip = first;
				}
			}
			//Calculating which vehicle type is involved in maximum accidents
			else if(str.startsWith("v4_")) {
				String temp = str.substring(3);
				char c = (char)(9);
				int index = temp.lastIndexOf(c);
				int index1 = temp.lastIndexOf(" ");
				index = Math.max(index, index1);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>VehicleTypeMaxAccidents) {
					VehicleTypeMaxAccidents = val;
					VehicleType = first;
				}
			}
			//Calculating year in which maximum Number Of Persons and Pedestrians Injured
			else if(str.startsWith("v5_")) {
				String temp = str.substring(3);
				char c = (char)(9);
				int index = temp.lastIndexOf(c);
				int index1 = temp.lastIndexOf(" ");
				index = Math.max(index, index1);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>YearPpInuredMaxAccidents) {
					YearPpInuredMaxAccidents = val;
					YearPpInured = first;
				}
			}
			//Calculating year in which maximum Number Of Persons and Pedestrians Killed
			else if(str.startsWith("v6_")) {
				String temp = str.substring(3);
				char c = (char)(9);
				int index = temp.lastIndexOf(c);
				int index1 = temp.lastIndexOf(" ");
				index = Math.max(index, index1);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>YearPpKilledMaxAccidents) {
					YearPpKilledMaxAccidents = val;
					YearPpKilled = first;
				}
			}
			//Calculating year in which maximum Number Of Cyclist Injured and Killed (combined)
			else if(str.startsWith("v7_")) {
				String temp = str.substring(3);
				char c = (char)(9);
				int index = temp.lastIndexOf(c);
				int index1 = temp.lastIndexOf(" ");
				index = Math.max(index, index1);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>cyclistMaxAccidents) {
					cyclistMaxAccidents = val;
					cyclist = first;
				}
			}
			//Calculating year in which maximum Number Of Motorist Injured and Killed (combined)
			else if(str.startsWith("v8_")) {
				String temp = str.substring(3);
				char c = (char)(9);
				int index = temp.lastIndexOf(c);
				int index1 = temp.lastIndexOf(" ");
				index = Math.max(index, index1);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>motoristsMaxAccidents) {
					motoristsMaxAccidents = val;
					motorists = first;
				}
			}
		}
		//Printing all the outputs
		System.out.print("1. Date on which maximum number of accidents took place:  ");
		System.out.println(Date + "  " + DateMaxAccidents);
		System.out.print("2. Borough with maximum count of accident fatality:  ");
		System.out.println(Borough + " " + BoroughMaxAccidents);
		System.out.print("3. Zip with maximum count of accident fatality:  ");
		System.out.println(Zip + " " + ZipMaxAccidents);
		System.out.print("4. Which vehicle type is involved in maximum accidents:  ");
		System.out.println(VehicleType + " " + VehicleTypeMaxAccidents);
		System.out.print("5. Year in which maximum Number Of Persons and Pedestrians Injured:  ");
		System.out.println(YearPpInured + " " + YearPpInuredMaxAccidents);
		System.out.print("6. Year in which maximum Number Of Persons and Pedestrians Killed:  ");
		System.out.println(YearPpKilled + " " + YearPpKilledMaxAccidents);
		System.out.print("7. Year in which maximum Number Of Cyclist Injured and Killed:  ");
		System.out.println(cyclist + " " + cyclistMaxAccidents);
		System.out.print("8. Year in which maximum Number Of Motorist Injured and Killed:  ");
		System.out.println(motorists + " " + motoristsMaxAccidents);
	}
}
