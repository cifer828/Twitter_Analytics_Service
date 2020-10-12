import java.sql.Types

import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._

/** ****************************************************************************
 *
 * @author Qiuchen Z.
 * @date 4/9/20
 * *****************************************************************************/


object SaicSparkJdbcDialect {
  def useMyJdbcDIalect(jdbcUrl:String,dbType:String): Unit = {

    // 将当前的 JdbcDialect 对象unregistered掉
    val dialect = JdbcDialects
    JdbcDialects.unregisterDialect(dialect.get(jdbcUrl))

    if (dbType.equals("MYSQL")) {
      val OracleDialect = new JdbcDialect {
        // 只能处理ORACLE数据库
        override def canHandle(url: String): Boolean = url.startsWith("jdbc:mysql")

        // 修改数据库 SQLType 到 Spark DataType 的映射关系（从数据库读取到Spark中）
        override def getCatalystType(sqlType: Int, typeName: String, size: Int,
                                     md: MetadataBuilder): Option[DataType] = {
          if (sqlType == Types.VARCHAR) {
            // 将不支持的 Timestamp with local Timezone 以TimestampType形式返回
            Some(TimestampType)
          } else {
            Some(StringType)
          }
        }

        // 该方法定义的是数据库Spark DataType 到 SQLType 的映射关系，此处不需要做修改
        override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
          case StringType => Some(JdbcType("VARCHAR2(2000)", java.sql.Types.LONGVARCHAR))
          case BooleanType => Some(JdbcType("NUMBER(1)", java.sql.Types.NUMERIC))
          case IntegerType => Some(JdbcType("NUMBER(10)", java.sql.Types.NUMERIC))
          case LongType => Some(JdbcType("NUMBER(19)", java.sql.Types.NUMERIC))
          case DoubleType => Some(JdbcType("NUMBER(19,4)", java.sql.Types.NUMERIC))
          case FloatType => Some(JdbcType("NUMBER(19,4)", java.sql.Types.NUMERIC))
          case ShortType => Some(JdbcType("NUMBER(5)", java.sql.Types.NUMERIC))
          case ByteType => Some(JdbcType("NUMBER(3)", java.sql.Types.NUMERIC))
          case BinaryType => Some(JdbcType("BLOB", java.sql.Types.BLOB))
          case TimestampType => Some(JdbcType("DATE", java.sql.Types.TIMESTAMP))
          case DateType => Some(JdbcType("DATE", java.sql.Types.DATE))
          case _ => None
        }

        override def quoteIdentifier(colName: String): String = {
          colName
        }
      }
      // register新创建的 JdbcDialect 对象
      JdbcDialects.registerDialect(OracleDialect)
    }
  }
}
