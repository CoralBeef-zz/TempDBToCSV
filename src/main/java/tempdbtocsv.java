import javax.xml.transform.Result;
import java.sql.*;

public class tempdbtocsv {

    Connection con = null;
    Connection conupload = null;
    Statement sta = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    String sql = "";
    int bcounter = 0;
    final int STACK_SIZE = 10000;
    final int START_FROM = 1;
    final int UP_TO = 1000000;

    private void go() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql:///crawlerling?useUnicode=true&characterEncoding=utf-8";

            con = DriverManager.getConnection(url,"root","");
            conupload = DriverManager.getConnection(url, "root", "");

            sta = con.createStatement();

            String columns = "`法人番号`,`更新年月日`,`商号又は名称`,`国内所在地（都道府県）`,`国内所在地（市区町村）`," +
            "`国内所在地（丁目番地等）`,`法人番号指定年月日`,`フリガナ`";
            String uploadcolumns = "`URL`,`法人番号`,`名称`,`住所`,`カナ`,`最終更新年月日`,`新規法人番号指定年月日`,`電話番号`,`県域`";

            sql = "INSERT INTO houjinbangouproduct ("+uploadcolumns+") VALUES (";
            String[] columnsx = uploadcolumns.split(",");
            for(int l = 0; l < columnsx.length; l++) sql += (l != (columnsx.length - 1)) ? "?," : "?);";
            ps = con.prepareStatement(sql);

            rs = sta.executeQuery("SELECT "+columns+" FROM `houjinbangou` WHERE id > "+UP_TO);//BETWEEN "+START_FROM+" AND "+UP_TO+";");

            while(rs.next()) {
                String corporate_number = rs.getString("法人番号");
                String prefecture_area = rs.getString("国内所在地（都道府県）");
                String address = prefecture_area + rs.getString("国内所在地（市区町村）") + rs.getString("国内所在地（丁目番地等）");

                ps.setString(1,"https://www.houjin-bangou.nta.go.jp/henkorireki-johoto.html?selHouzinNo="+corporate_number);
                ps.setString(2,corporate_number);
                ps.setString(3,rs.getString("商号又は名称"));
                ps.setString(4,address);
                ps.setString(5,rs.getString("フリガナ"));
                ps.setString(6,rs.getString("更新年月日"));
                ps.setString(7,rs.getString("法人番号指定年月日"));
                ps.setString(8," ");
                ps.setString(9,prefecture_area);

                System.out.println("https://www.houjin-bangou.nta.go.jp/henkorireki-johoto.html?selHouzinNo="+corporate_number);

                bcounter++;
                if(bcounter >= STACK_SIZE) {
                    ps.addBatch();
                    ps.executeLargeBatch();
                    ps.close();
                    ps = conupload.prepareStatement(sql);
                    bcounter = 0;
                }
                else ps.addBatch();
            }
            ps.executeLargeBatch();
            ps.close();

        } catch(SQLException | ClassNotFoundException exc) {
            System.out.println("ERROR: "+exc.toString());
        } finally {
            try {
                rs.close();
                sta.close();
                ps.close();
                con.close();
                conupload.close();
            } catch(SQLException sqle) {}
        }
    }

    public static void main(String[] args) {
        new tempdbtocsv().go();
    }
}
