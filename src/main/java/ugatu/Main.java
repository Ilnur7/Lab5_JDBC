package ugatu;

import java.sql.SQLException;

public class Main {
    public static void main(String[] args) {
        DBTester dbTester = new DBTester();
        try {
            dbTester.test();
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
