package ugatu;
import java.io.*;
import java.sql.*;
import java.util.HashSet;
import java.util.Set;

public class DBTester {

    private final String user = "postgres";
    private final String password = "4444";
    private final String url = "jdbc:postgresql://localhost:5432/groupdb";

    private static Connection connection;

    public Connection getConnection() {
        return connection;
    }

    public void test() throws SQLException, ClassNotFoundException {
        connection = connectToDb();
        doWork(connection);
        connection.close();
    }

    private void doWork(Connection connection) throws SQLException {
        System.out.println("Задание 6");
        viewGroups();
        viewItems();

        System.out.println("Задание 7");
        System.out.println("id: " + getGroupId("Фрукты"));

        System.out.println("Задание 6*");
        printResultSet();

        System.out.println("Задание 8");
        viewItemsInGroup(1);
        viewItemsInGroup("Овощи");

        System.out.println("Задание 9");
        createTablesIfNeeded();
        System.out.println("Задание 10");
        addItemToGroup("Вишня", "Ягоды");
        removeItemFromGroup("Вишня", "Ягоды");
        System.out.println("Задание 11");
        readFileItem("items");
        //System.out.println("Задание 12");
        //readFileGroup("groups");
    }

    private Connection connectToDb() throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        Connection connection = DriverManager.getConnection(url, user, password);
        return connection;
    }

    //6
    private void viewGroups() throws SQLException {
        try (Statement statement = getConnection().createStatement();
             ResultSet resultSet = statement.executeQuery("select * from itemgroup")) {
            while (resultSet.next()) {
                String nameGroup = "Название группы: " + resultSet.getString("title");
                System.out.println(nameGroup);
            }
        }
    }

    //6
    private void viewItems() throws SQLException {
        try (Statement statement = getConnection().createStatement();
             ResultSet resultSet = statement.executeQuery("select * from item");){
            System.out.println();
            while (resultSet.next()) {
                String nameItem = "Название съедобных растений: " + resultSet.getString("title");
                System.out.println(nameItem);
            }
        }
    }

    //6*
    private void printResultSet() throws SQLException {
        try (ResultSet resultSet = getConnection().createStatement().executeQuery("SELECT * FROM itemgroup");) {
            ResultSetMetaData meta = resultSet.getMetaData();
            int columnCount = meta.getColumnCount();
            while (resultSet.next()){
                for (int i = 1; i <= columnCount; i++) {
                    String nameColumn = meta.getColumnName(i);
                    System.out.println(nameColumn + ": " + resultSet.getString(nameColumn));
                }
            }
        }
    }

    //7
    private int getGroupId(String key) throws SQLException {
        ResultSet resultSet = null;
        int id = 0;
        try (PreparedStatement statement = getConnection().prepareStatement("SELECT * FROM itemgroup WHERE title = (?)");) {
            statement.setString(1, key);
            resultSet = statement.executeQuery();
            if (resultSet.next()) {
                id = resultSet.getInt("id");
            }
        }
        return id;
    }

    //8
    private void viewItemsInGroup (int idItemGroup) throws SQLException {
        ResultSet resultSet = null;
        try (PreparedStatement statement = getConnection().prepareStatement("SELECT * FROM item INNER JOIN itemgroup ON item.groupid = ? AND itemgroup.id =?");) {
            statement.setInt(1, idItemGroup);
            statement.setInt(2, idItemGroup);
            resultSet = statement.executeQuery();
            System.out.println("Группе с id = " + idItemGroup + " соответствуют растения:");
            while (resultSet.next()){
                int id = resultSet.getInt("id");
                int groupid = resultSet.getInt("groupid");
                String name = resultSet.getString("title");
                System.out.println("Название: " + name + ", id = " + id + ", groupid = " + groupid);
            }
        }catch (SQLException e){
            e.printStackTrace();
        }finally {
            resultSet.close();
        }
    }

    //8
    private void viewItemsInGroup (String groupname) {
        try {
            int idItemGroup = getGroupId(groupname);
            viewItemsInGroup(idItemGroup);
            System.out.println("\nНазвание группы: " + groupname);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    //9
    private void createTablesIfNeeded() throws SQLException {
        String createItem = "CREATE TABLE IF NOT EXISTS ITEMGROUP (" +
                "  ID SERIAL PRIMARY KEY," +
                "  TITLE VARCHAR(100)  UNIQUE NOT NULL)";
        String createItemGroup = "CREATE TABLE IF NOT EXISTS ITEM (" +
                "  ID SERIAL PRIMARY KEY," +
                "  TITLE VARCHAR(100) UNIQUE NOT NULL," +
                "  GROUPID INTEGER," +
                "  FOREIGN KEY (GROUPID) REFERENCES ITEMGROUP(ID) )";

        Statement statement = getConnection().createStatement();
        try {
            statement.executeQuery("SELECT * FROM item");
            statement.executeQuery("SELECT * FROM itemgroup");
        } catch (SQLException e) {
            System.out.println("Нет такой таблицы, создаем новую");
            statement.executeUpdate(createItem);
            statement.executeUpdate(createItemGroup);
        } finally {
            statement.close();
        }
    }
    //10
    private boolean addItemToGroup(String itemName, String groupName){
        try (PreparedStatement preparedStatement = getConnection().prepareStatement("INSERT INTO ITEM(TITLE,GROUPID) VALUES(?, ?)")) {
            preparedStatement.setString(1, itemName);
            preparedStatement.setInt(2, getGroupId(groupName));
            preparedStatement.executeUpdate();
            System.out.println("\nРастение: " + itemName + " добавлено в группу " + groupName);
            return true;
        } catch (SQLException e) {
            System.out.println("\nРастение: " + itemName + " не удалось добавить в группу " + groupName);
            e.printStackTrace();
            return false;
        }
    }

    private boolean removeItemFromGroup(String itemName, String groupName){
        try (PreparedStatement preparedStatement = getConnection().prepareStatement("DELETE FROM ITEM WHERE TITLE = ? AND GROUPID = ?")) {
            preparedStatement.setString(1, itemName);
            preparedStatement.setInt(2, getGroupId(groupName));
            preparedStatement.executeUpdate();
            System.out.println("\nРастение: " + itemName + " удалено из группы " + groupName);
            return true;
        } catch (SQLException e) {
            System.out.println("\nРастение: " + itemName + ", не удалось удалить из группы " + groupName);
            e.printStackTrace();
            return false;
        }
    }

    private void readFileItem(String fileName) throws SQLException {
        File file = new File(fileName);
        int lineCount = 0;
        boolean isEmptyTransaction = false;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "utf-8"))) {
            String s;
            getConnection().setAutoCommit(false); //операции ниже в бд не будут выполняться пока их не закоммитить
            while ((s = reader.readLine()) != null ) {
                lineCount++;
                String [] arrStr = null;
                if (s.contains("-")){
                    arrStr = s.split("-");
                    boolean isRemoved = removeItemFromGroup(arrStr[1], arrStr[0]);
                    if (!(isRemoved)) {
                        System.out.println("\nРастение: " + arrStr[1] + ", не удалось удалить из группы " + arrStr[0]);
                        getConnection().rollback();
                        isEmptyTransaction = true;
                        System.out.println("Транзакция отменена, ошибка при удалении кортежа");
                        return;
                    }
                }else if (s.contains("+")){
                    arrStr = s.split("\\+");
                    boolean isAdded = addItemToGroup(arrStr[1], arrStr[0]);
                    if (!(isAdded)){
                        System.out.println("\nРастение: " + arrStr[1] + ", не удалось добавить в группу " + arrStr[0]);
                        getConnection().rollback();
                        isEmptyTransaction = true;
                        System.out.println("Транзакция отменена, ошибка при добавлении кортежа");
                        return;
                    }
                }
            }
            if (!(isEmptyTransaction)){
                getConnection().commit();
            }
        } catch (IOException | SQLException e) {
            getConnection().rollback();
            System.out.println("Error in line " + lineCount);
            e.printStackTrace();
        } finally {
            getConnection().setAutoCommit(true);
        }

    }

    private void readFileGroup(String fileName) throws SQLException {
        File file = new File(fileName);
        int lineCount = 0;
        Set<String> setRemoveGroup = new HashSet<>();
        Set<String> setAddGroup = new HashSet<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "utf-8"))) {
            String s;
            while ((s = reader.readLine()) != null ) {
                lineCount++;
                if (s.charAt(0) == '-'){
                    setRemoveGroup.add(s.substring(1));
                }else if (s.charAt(0) == '+'){
                    setAddGroup.add(s.substring(1));
                }
            }
        } catch (IOException e) {
            System.out.println("Error in line: " + lineCount);
            e.printStackTrace();
        }

        try (Statement statement = getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
             ResultSet resultSet = statement.executeQuery( "SELECT * FROM itemgroup")){
            getConnection().setAutoCommit(false); //операции ниже в бд не будут выполняться пока их не закоммитить
            while (resultSet.next()){
                String nameGroup = resultSet.getString("title");
                if (setAddGroup.contains(nameGroup)){
                    setAddGroup.remove(nameGroup);
                }else if (setRemoveGroup.contains(nameGroup)){
                    resultSet.deleteRow();
                }
            }

            for (String nameAddGroup : setAddGroup) {
                try {
                    resultSet.moveToInsertRow();
                    resultSet.updateString("title", nameAddGroup);
                    resultSet.insertRow();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            getConnection().commit();
        } catch (SQLException e) {
            getConnection().rollback();
            System.out.println("Error");
            e.printStackTrace();
        } finally {
            getConnection().setAutoCommit(true);
        }
    }
}
