package network;

import java.sql.*;
import java.util.Scanner;

import simpledb.jdbc.network.NetworkDriver;

public class SectionInfo {
	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);

		System.out.print("Enter a section number: ");
		int sectionNumber = input.nextInt();

		input.close();

		String url = "jdbc:simpledb://localhost";
		StringBuilder sb = new StringBuilder();
		sb.append("select Prof from SECTION where SectId = ");
		sb.append(sectionNumber);
		String sectionQuery = sb.toString();

		Driver d = new NetworkDriver();
		try {
			Connection conn = d.connect(url, null);
			Statement stmt = conn.createStatement();
			ResultSet sectionResultSet = stmt.executeQuery(sectionQuery);
			boolean doesExist = sectionResultSet.next();
			if (!doesExist)
				System.out.println("No such sections.");
			else {
				String prof = sectionResultSet.getString("Prof");
				sectionResultSet.close();

				StringBuilder sb1 = new StringBuilder();
				sb1.append("select Grade from ENROLL where SectionId = ");
				sb1.append(sectionNumber);
				String enrollQuery = sb1.toString();
				ResultSet enrollResultSet = stmt.executeQuery(enrollQuery);

				int numberOfStudents = 0;
				int numberOfAs = 0;
				while (enrollResultSet.next()) {
					String grade = enrollResultSet.getString("Grade");
					if (grade.equals("A")) {
						numberOfAs++;
					}
					numberOfStudents++;
				}

				StringBuilder sb2 = new StringBuilder();
				sb2.append("Proffessor ");
				sb2.append(prof);
				sb2.append(" had ");
				sb2.append(numberOfStudents);
				sb2.append(" student(s) and gave ");
				sb2.append(numberOfAs);
				sb2.append(" A's.");
				System.out.println(sb2.toString());

				enrollResultSet.close();
				stmt.close();
				conn.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
