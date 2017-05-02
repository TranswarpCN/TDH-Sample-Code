import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBC_basic {
    private static String connection_url = "";
    private static Connection connection = null;

    public static void main(String[] args) {
        Constant constant = new Constant();
        try {
            Class.forName(constant.driverName);
            switch (constant.mode) {
                default:
                case "simple":
                    connection_url = constant.JDBC_URL;
                    connection = DriverManager.getConnection(connection_url);
                    break;
                case "ldap":
                    connection_url = constant.JDBC_URL;
                    connection = DriverManager.getConnection(connection_url, constant.UserName, constant.Password);
                    break;
                case "kerberos":
                    connection_url = constant.JDBC_URL + ";" + constant.Kerberos_Parameter;
                    connection = DriverManager.getConnection(connection_url);
                    break;
            }
            Statement stmt = connection.createStatement();
            String[] SQL = constant.RunSQL.split(";");
            for (String s : SQL)
                stmt.execute(s);
            stmt.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
