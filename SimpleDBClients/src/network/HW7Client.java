package network;
import java.sql.*;

import simpledb.jdbc.network.NetworkDriver;

public class HW7Client {
   public static void main(String[] args) {
      Driver d = new NetworkDriver();
      String url = "jdbc:simpledb://localhost";

      try (Connection conn = d.connect(url, null); 
            Statement stmt = conn.createStatement()) {
         String updateCmd = "update STUDENT set GradYear = null "
               + "where SName = 'amy'";
         stmt.executeUpdate(updateCmd);
         System.out.println("Amy's GradYear is now NULL.");
         
         String insertCmd = "insert into STUDENT(SId, SName, MajorId, GradYear) values "
        		 + "(99, 'tom', 20, null)";
         stmt.executeUpdate(insertCmd);
         System.out.println("Tom(99) is inserted with MajorId=20 and GradYear is NULL.");
         
         String selectQuery = "select sname from student "
                 + "where GradYear > 2019 "
                 + "and GradYear < 2022";
         ResultSet rs = stmt.executeQuery(selectQuery);
         System.out.println("Students graduating after 2019 and before 2022:");
	     while (rs.next()) {
	        String sname = rs.getString("sname");
	        System.out.println(sname);
	     }
	     
	     String selectQuery2 = "select sname from student "
                 + "where GradYear is null ";
         ResultSet rs2 = stmt.executeQuery(selectQuery2);
         System.out.println("Students have GradYear=null:");
	     while (rs2.next()) {
	        String sname = rs2.getString("sname");
	        System.out.println(sname);
	     }
      }
      catch(SQLException e) {
         e.printStackTrace();
      }
   }
}
