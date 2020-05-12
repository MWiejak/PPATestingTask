import java.sql.*;

import com.sun.xml.internal.ws.developer.UsesJAXBContext;

import org.junit.jupiter.api.*;

import static java.sql.DriverManager.getConnection;

/**
 * Created by pwilkin on 27-Apr-20.
 */
public class SimpleDbTest {

    private static final String DBDESC = "jdbc:hsqldb:mem:test";

    @BeforeAll
    public static void prepareDatabase() {
        try (Connection c = getConnection(DBDESC, "SA", "")) {
            c.createStatement().execute("CREATE TABLE TESTING (ID INT PRIMARY KEY IDENTITY, TCOL VARCHAR(255), NUM DECIMAL(8, 2))");
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO TESTING (TCOL, NUM) VALUES (?, ?)")) {
                ps.setString(1, "val1");
                ps.setDouble(2, 4.2);
                ps.execute();
                ps.setString(1, "val2");
                ps.setDouble(2, 5.0);
                ps.execute();
                ps.setString(1, "val3");
                ps.setDouble(2, -4.3);
                ps.execute();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public static void destroyDatabase() {
        try (Connection c = getConnection(DBDESC, "SA", "")) {
            c.createStatement().execute("DROP TABLE TESTING");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void testConnection() {
        try (Connection c = getConnection(DBDESC, "SA", "")) {
            c.createStatement().executeQuery("SELECT * FROM TESTING");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testThreeEntries() {
        try (Connection c = getConnection(DBDESC, "SA", "")) {
            int cnt = 0;
            try (ResultSet rs = c.createStatement().executeQuery("SELECT * FROM TESTING")) {
                while (rs.next()) {
                    cnt++;
                }
            }
            Assertions.assertEquals(cnt, 3);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // sprawdzic, czy uda sie dodac jeden nowy wpis do tabeli

    @Test
    public void testAdd() {

        try (Connection c = getConnection(DBDESC, "SA", "")) {

            String sql= "INSERT INTO TESTING (TCOL, NUM) VALUES (4, 4)";

            int cnt = 0;
            try (ResultSet rs = c.createStatement().executeQuery("SELECT * FROM TESTING")) {
                while (rs.next()) {
                    cnt++;
                }
            }

            c.createStatement().execute(sql);

            int cntnew = 0;
            try (ResultSet rs = c.createStatement().executeQuery("SELECT * FROM TESTING")) {
                while (rs.next()) {
                    cntnew++;
                }
            }

            Assertions.assertEquals(cnt+1,cntnew);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    // sprawdzic, czy uda sie usunac jeden wpis z tabeli

    @Test
    public void testRemove() {

        try (Connection c = getConnection(DBDESC, "SA", "")) {

            String sql= "DELETE FROM TESTING WHERE ID=1 ";

            int cnt = 0;
            try (ResultSet rs = c.createStatement().executeQuery("SELECT * FROM TESTING")) {
                while (rs.next()) {
                    cnt++;
                }
            }

            c.createStatement().execute(sql);

            int cntnew = 0;
            try (ResultSet rs = c.createStatement().executeQuery("SELECT * FROM TESTING")) {
                while (rs.next()) {
                    cntnew++;
                }
            }

            Assertions.assertEquals(cnt-1,cntnew);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // sprawdzic, czy po dodaniu jednego wpisu i usunieciu jednego wpisu nadal w tabeli sa trzy wpisy

    @Test
    public void testThree(){

        try (Connection c = getConnection(DBDESC, "SA", "")) {

            String insert= "INSERT INTO TESTING (TCOL, NUM) VALUES (4, 4)";
            String remove= "DELETE FROM TESTING WHERE ID=1 ";

            int cnt = 0;
            try (ResultSet rs = c.createStatement().executeQuery("SELECT * FROM TESTING")) {
                while (rs.next()) {
                    cnt++;
                }
            }

            c.createStatement().execute(insert);
            c.createStatement().execute(remove);

            int cntnew = 0;
            try (ResultSet rs = c.createStatement().executeQuery("SELECT * FROM TESTING")) {
                while (rs.next()) {
                    cntnew++;
                }
            }

            Assertions.assertEquals(cnt,cntnew);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    // sprawdzic, czy po dodaniu wpisu z wartoscia 10.0 maksymalna wyciagnieta wartosc (podpowiedz: SELECT MAX(NUM) ...) wynosi 10.0)
    @Test
    public void testMax() {
        try (Connection c = getConnection(DBDESC, "SA", "")) {

            String sql = "INSERT INTO TESTING (TCOL, NUM) VALUES (0, 10.0)";

            c.createStatement().execute(sql);

            ResultSet resultSet= c.createStatement().executeQuery("SELECT MAX(NUM) FROM TESTING");

            double result=0;

            while (resultSet.next()) result = resultSet.getDouble("C1");

            double epislon = 0.0001;



            Assertions.assertTrue(Math.abs(result-10.0)<epislon);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
