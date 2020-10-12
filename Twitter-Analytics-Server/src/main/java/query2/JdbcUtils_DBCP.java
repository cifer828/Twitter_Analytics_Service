package query2;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;
import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;

public class JdbcUtils_DBCP {
    private static DataSource ds = null;
    private static BasicDataSource bds = null;
    static{
        try{
            // dbcp configure
            bds = new BasicDataSource();

            String myDB = System.getenv("MYSQL_DB");
            String username = System.getenv("MYSQL_USER");
            String pwd = System.getenv("MYSQL_PWD");

            bds.setUrl("jdbc:mysql://" + myDB + "/all_db?serverTimezone=UTC");
            bds.setUsername(username);
            bds.setPassword(pwd);
            // allow pool prepared statement to improve performance
            bds.setPoolPreparedStatements(true);
            bds.setInitialSize(10);
            bds.setMaxActive(50);
            bds.setMaxIdle(30);
            bds.setMaxWait(1000);
        }catch (Exception e) {
            System.out.println("Load config error: " + e.getMessage());
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Get the proxy connection
     *
     * @return proxy connection
     * @throws SQLException deal with sql issues
     */
    public static Connection getConnection() throws SQLException{
        return bds.getConnection();
    }

    /**
     * Prepared statement for "both" type query
     *
     * @param conn proxy connection
     * @return proxy prepared statement
     * @throws SQLException deal with sql issues
     */
    public static PreparedStatement bothPreStatement(Connection conn) throws SQLException{
        return conn.prepareStatement("SELECT * FROM python_combine_all WHERE uid = ?");
    }

    /**
     * Prepared statement for "retweet" or "reply" type query
     *
     * @param conn proxy connection
     * @return proxy prepared statement
     * @throws SQLException deal with sql issues
     */
    public static PreparedStatement singleStatement(Connection conn) throws SQLException{
        return conn.prepareStatement("SELECT * FROM python_combine_all WHERE uid = ? AND type = ?");
    }

    /**
     * Close connection, statement and result set.
     * Not really close, just reture the connection and statement to the pool.
     *
     * @param conn proxy connection
     * @param st proxy prepared statement
     * @param rs result set
     */
    public static void release(Connection conn,Statement st,ResultSet rs){
        if(rs!=null){
            try{
                rs.close();
            }catch (Exception e) {
                e.printStackTrace();
            }
            rs = null;
        }
        if(st!=null){
            try{
                st.close();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }

        if(conn!=null){
            try{
                conn.close();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
