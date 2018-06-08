package hive.essentials.hiveudf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

@Description(
        name = "toUpper",
        value="_FUNC_(value) - Returns upper case of value.",
        extended="Example:\n"
                + "  > SELECT _FUNC_('will');\n"
                + "  WILL")
public final class ToUpper extends UDF {
  public Text evaluate(final Text s) {
    if (s == null) { return null; }
    return new Text(s.toString().toUpperCase());
  }
}
