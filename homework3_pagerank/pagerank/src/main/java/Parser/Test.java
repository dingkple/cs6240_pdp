package Parser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by kingkz on 10/16/16.
 */
public class Test {

    static class Point {
        int x = 0;
        int y = 1;

        public Point(int x, int y) {
            this.x = x;
            this.y = y;
        }
    }

    private static Pattern p;
    public static void main(String[] args) {
        p = Pattern.compile("^([^~|\\?]+)$");

        ArrayList<String> tests = new ArrayList<>();
        tests.add("ccsn-sdfn");
        tests.add("aaa");
        tests.add("what?");


        ArrayList<Point> ap = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Random r = new Random();
            int a = r.nextInt(10);
            int b = r.nextInt(10);

            Point p = new Point(a, b);
            ap.add(p);
        }

//        for (String s : tests) {
//            Matcher matcher = p.matcher(s);
//            while (matcher.find()) {
//                for (int i = 0; i < matcher.groupCount(); i++) {
//                    System.out.println(matcher.group(i));
//                }
//            }
//        }

//        Iterator<Point> vals = ap.iterator();

        ArrayList<Point> objs= new ArrayList<Point>();
        //loading in a list
//        Point p;

        Iterable<Point> iap = (Iterable<Point>)ap;
        for (Point p : iap) {
//            p = vals.next();
            System.out.println("Here is Point In Iterator: "+ p.x + " " + p.y);
            objs.add(p);
        }
        //reading from list
        for(Point obj: objs) {
            System.out.println("Here is MyObject In List: " + obj.x + " " + obj.y);
        }
    }
}
