package package1;

import java.sql.*;
import java.util.concurrent.ExecutionException;

public class MysqlCon{
    public static int st_id;
    public static String st_name;
    public static String st_status;

    public static void main(String args[]) throws ExecutionException, InterruptedException {
        try{
            Class.forName("com.mysql.jdbc.Driver");
            Connection con=DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/student","root","");

            Statement stmt=con.createStatement();
            ResultSet rs=stmt.executeQuery("select * from student_info");
            while(rs.next())
                System.out.println(rs.getInt(1)+"  "+rs.getString(2)+"  "+rs.getString(3));
            st_id = rs.getInt(1);
             st_name= rs.getString(2);
            st_status = rs.getString(3);

            con.close();
        }catch(Exception e){ System.out.println(e);}
    }
}