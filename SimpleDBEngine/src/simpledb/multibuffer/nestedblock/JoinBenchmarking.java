package simpledb.multibuffer.nestedblock;

import simpledb.buffer.BufferMgr;
import simpledb.jdbc.embedded.EmbeddedDriver;
import simpledb.opt.TablePlanner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.*;

public class JoinBenchmarking {

    static boolean disableIndexing = false;
    static ResultSet rs;
    static HashMap<String, Integer> matches = new HashMap<>();
    static ArrayList<String> searchkeys = new ArrayList<>();

    public static long joinTableTest(int n) throws SQLException {
        EmbeddedDriver d = new EmbeddedDriver();
        String url = "jdbc:simpledb:studentdb" + UUID.randomUUID().toString().substring(9); //Makes new database each time
        Connection conn = d.connect(url, null);
        Statement stmt = conn.createStatement();

        /* TABLE BUILDING */
        String s = "create table STUDENT(SId int, SFirstName varchar(40), " +
                "SLastName varchar(40), MajorId int, GradYear int)";
        stmt.executeUpdate(s);
        System.out.println("Table STUDENT created.");

        ArrayList<Name> names = Name.generateNames(n);

        s = "insert into STUDENT(SId, SFirstName, SLastName, MajorId, GradYear) values ";
        String[] studvals = new String[n];

        for (int i = 0; i < n; i++) {
            studvals[i] = String.format("(%d, '%s', '%s', %d, %d)", (i + 1), names.get(i).firstName,
                    names.get(i).lastName, rand.nextInt(courseNames.length), randomGradYear());
        }
        for (String studval : studvals) stmt.executeUpdate(s + studval);
        System.out.println("STUDENT records inserted.");

        if (!disableIndexing) {
            // Create an index on the student major id
            s = "create index smid on STUDENT(MajorId)";
            stmt.executeUpdate(s);
        }

        s = "create table MAJOR(MId int, MajorName varchar(40), MajorAbbr varchar(5))";
        stmt.executeUpdate(s);
        System.out.println("Table MAJOR created.");

        s = "insert into MAJOR(MId, MajorName, MajorAbbr) values ";
        String[] majorvals = new String[courseNames.length];
        for (int i = 0; i < courseNames.length; i++) {
            majorvals[i] = String.format("(%d, '%s', '%s')", i, courseNames[i], courseAbs[i]);
        }

        for (String majorval : majorvals) stmt.executeUpdate(s + majorval);
        System.out.println("MAJOR records inserted.");

        if (!disableIndexing) {
            // Create an index on the major id
            s = "create index mid on MAJOR(MId)";
            stmt.executeUpdate(s);
        }

        /* RUNNING JOIN QUERY */
        s = "select SId, SFirstName, SLastName, MId, MajorName, MajorAbbr " +
                "from STUDENT, MAJOR " +
                "where MId = MajorId";

        BufferMgr.hits = 0;
        BufferMgr.misses = 0;
        long time = System.currentTimeMillis();
        rs = stmt.executeQuery(s);
        int count = 0;
        while (rs.next()) {
            count++;
            String result = rs.getInt("SId") + rs.getString("SFirstName") +
                    rs.getString("SLastName") + rs.getInt("MId") +
                    rs.getString("MajorName") + rs.getString("MajorAbbr");
            matches.put(result, matches.getOrDefault(result, 0) + 1);
            searchkeys.add(result);
        } // Going through entire result set.

        System.out.println("Number of records in join: " + count);
        conn.close();
        return System.currentTimeMillis() - time;
    }

    private static String runTest(int n, int type) {
        TablePlanner.DEBUG_MODE = true;
        TablePlanner.MODE = type;
        String test = "";
        String result = "";
        switch (type) {
            case 1:
                test = "Index Join";
                break;
            case 2:
                test = "Block Nested Loop Join";
                break;
            case 3:
                test = "Multi Buffer Product and Select";
        }
        try {
            String csvForm = "";
            long time = joinTableTest(n);
            result += (test + " on input size: " + n + "\n");
            System.out.println(test + " time taken: " + time);
            result += (test + " guess for block accesses: " + TablePlanner.DEBUG_PLAN.blocksAccessed() + "\n");
            result += (test + " hits: " + BufferMgr.hits) + "\n";
            result += (test + " misses (disk reads): " + BufferMgr.misses) + "\n" + "\n";
            if (TablePlanner.MODE != type) {
                return test + " mode was not possible.\n\n";
            }
            csvForm += n + "," + test + "," + time + "," + TablePlanner.DEBUG_PLAN.blocksAccessed() + "," +
                    BufferMgr.hits + "," + BufferMgr.misses;
            return csvForm;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) throws SQLException {
//        runTest(300, 1); // Index Join
//        runTest(300, 2); // Block Nested Loop Join
//        runTest(300, 3); // Multi Buffer Product and Select
//        runTests();
//        correctnessTest();
//        writeToFile();
//        testSelection();
    }

    public static void testSelection() throws SQLException {
        TablePlanner.DEBUG_MODE = false;
        disableIndexing = true;
        try {
            String csvForm = "";
            long time = joinTableTest(200);
            if (TablePlanner.MODE == 2) {
                System.out.println("TablePlanner picked Block Nested Join over Cross Product");
            } else {
                System.out.println("TablePlanner failed to pick Block Nested Join over Cross Product");
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    private static void correctnessTest() throws SQLException {
        matches = new HashMap<>();
        searchkeys = new ArrayList<>();

        runTest(100, 2); // Block Nested Loop Join
        runTest(100, 3); // Multi Buffer Product and Select

        int count = 2; // All the three joins should have the same results => Same tuples
        for (String key : searchkeys) {
             if (matches.get(key) != count) { // Check if all the results are the same
                 System.out.println("Incorrect results for " + key);
                 return;
             }
        }
        System.out.println("All tests passed.");
    }

    private static void runTests() {
        int[] sizes = new int[]{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000};
        StringBuilder s1 = new StringBuilder();
        for (int size : sizes) {
            for (int j = 1; j <= 3; j++) {
                s1.append(runTest(size, j));
            }
        }
        System.out.println("\n\n---FINAL RESULTS---");
        System.out.print(s1);
    }

    private static void writeToFile() {
        // Change the runTest method to return csv form for this to work
        int[] sizes = new int[]{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000};
        PrintWriter pw;
        try {
            pw = new PrintWriter("results.csv");
            pw.println("Input Size,Join Algorithm,Runtime,Block Guess,Hits,Misses");
            for (int size : sizes) {
                for (int j = 1; j <= 3; j++) {
                    pw.println(runTest(size, j));
                }
            }
            pw.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    static Random rand = new Random(42); // For number generation


    private static int randomGradYear() {
        return 2021 + rand.nextInt(4);
    }

    private static final String[] courseNames = new String[]{"Computer Science", "Chemical Engineering",
            "Mechanical Engineering", "Aerospace Engineering"};

    private static final String[] courseAbs = new String[]{"CS", "CHE", "ME", "ASE"};

    private static class Name {
        String firstName;
        String lastName;

        public Name(String firstName, String lastName) {
            this.firstName = firstName;
            this.lastName = lastName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Name name = (Name) o;
            return Objects.equals(firstName, name.firstName) && Objects.equals(lastName, name.lastName);
        }

        public static ArrayList<Name> generateNames(int n) {
            rand = new Random(42); // Fix the seed
            ArrayList<Name> names = new ArrayList<>();
            int c1 = 0;
            int c2 = 0;
            n = Math.min(n, firstNameList.length * lastNameList.length);
            for (int i = 0; i < n; i++) {
                Name name = new Name(firstNameList[rand.nextInt(firstNameList.length)],
                        lastNameList[rand.nextInt(lastNameList.length)]);
                while (names.contains(name)) {
                    name = new Name(firstNameList[rand.nextInt(firstNameList.length)],
                            lastNameList[rand.nextInt(lastNameList.length)]);
                }
                names.add(name);
            }
            return names;
        }

        static final String[] firstNameList = new String[]{"Adam", "Adrian", "Alan", "Alexander", "Andrew", "Anthony", "Austin",
                "Benjamin", "Blake", "Boris", "Brandon", "Brian", "Cameron", "Carl", "Charles", "Christian", "Christopher",
                "Colin", "Connor", "Dan", "David", "Dominic", "Dylan", "Edward", "Eric", "Evan", "Frank", "Gavin", "Gordon",
                "Harry", "Ian", "Isaac", "Jack", "Jacob", "Jake", "James", "Jason", "Joe", "John", "Jonathan", "Joseph",
                "Joshua", "Julian", "Justin", "Keith", "Kevin", "Leonard", "Liam", "Lucas", "Luke", "Matt", "Max",
                "Michael", "Nathan", "Neil", "Nicholas", "Oliver", "Owen", "Paul", "Peter", "Phil", "Piers",
                "Richard", "Robert", "Ryan", "Sam", "Sean", "Sebastian", "Simon", "Stephen", "Steven",
                "Stewart", "Thomas", "Tim", "Trevor", "Victor", "Warren", "William"};

        static final String[] lastNameList = new String[]{"Abraham", "Allan", "Alsop", "Anderson", "Arnold", "Avery", "Bailey",
                "Baker", "Ball", "Bell", "Berry", "Black", "Blake", "Bond", "Bower", "Brown", "Buckland", "Burgess",
                "Butler", "Cameron", "Campbell", "Carr", "Chapman", "Churchill", "Clark", "Clarkson", "Coleman", "Cornish",
                "Davidson", "Davies", "Dickens", "Dowd", "Duncan", "Dyer", "Edmunds", "Ellison", "Ferguson", "Fisher",
                "Forsyth", "Fraser", "Gibson", "Gill", "Glover", "Graham", "Grant", "Gray", "Greene", "Hamilton",
                "Hardacre", "Harris", "Hart", "Hemmings", "Henderson", "Hill", "Hodges", "Howard", "Hudson", "Hughes",
                "Hunter", "Ince", "Jackson", "James", "Johnston", "Jones", "Kelly", "Kerr", "King", "Knox", "Lambert",
                "Langdon", "Lawrence", "Lee", "Lewis", "Lyman", "MacDonald", "Mackay", "Mackenzie", "MacLeod", "Manning",
                "Marshall", "Martin", "Mathis", "May", "McDonald", "McLean", "McGrath", "Metcalfe", "Miller", "Mills",
                "Mitchell", "Morgan", "Morrison", "Murray", "Nash", "Newman", "Nolan", "North", "Ogden", "Oliver", "Paige",
                "Parr", "Parsons", "Paterson", "Payne", "Peake", "Peters", "Piper", "Poole", "Powell", "Pullman", "Quinn",
                "Rampling", "Randall", "Rees", "Reid", "Roberts", "Robertson", "Ross", "Russell", "Rutherford", "Sanderson",
                "Scott", "Sharp", "Short", "Simpson", "Skinner", "Slater", "Smith", "Springer", "Stewart", "Sutherland",
                "Taylor", "Terry", "Thomson", "Tucker", "Turner", "Underwood", "Vance", "Vaughan", "Walker", "Wallace",
                "Walsh", "Watson", "Welch", "White", "Wilkins", "Wilson", "Wright", "Young"};
    }
}
