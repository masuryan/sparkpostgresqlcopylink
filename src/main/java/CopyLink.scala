import java.io.InputStream
import java.sql.DriverManager
import java.io.InputStream
import java.sql.DriverManager
import java.util.Properties
import java.util.Properties
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.Row
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection
import org.apache.spark.sql.SaveMode

object CopyLink extends Serializable {

  def rowsToInputStream(rows: Iterator[Row]): InputStream = {
    val bytes: Iterator[Byte] = rows.map { row =>
      (row.toSeq
        .map { v =>
          if (v == null) {
            """\N"""
          } else {
            "\"" + v.toString.replaceAll("\"", "\"\"") + "\""
          }
        }
        .mkString("\t") + "\n").getBytes
    }.flatten

    new InputStream {
      override def read(): Int =
        if (bytes.hasNext) {
          bytes.next & 0xff // make the signed byte an unsigned int
        } else {
          -1
        }
    }
  }

  def copyIn(url: String, df: DataFrame, table: String):Unit = {
    var cols = df.columns.mkString(",")

    df.rdd.foreachPartition { rows =>
      val conn = DriverManager.getConnection(url)
      try {
        val cm = new CopyManager(conn.asInstanceOf[BaseConnection])
        cm.copyIn(
          s"COPY $table ($cols) " + """FROM STDIN WITH (NULL '\N', FORMAT CSV, DELIMITER E'\t')""",
          rowsToInputStream(rows))
        ()
      } finally {
        conn.close()
      }
    }
  }
}