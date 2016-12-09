package Utils

import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by kple on 12/2/16.
  */
object Utils {

  def checkoutput(fileSystem: FileSystem, output: String): Unit = {
    if (fileSystem.exists(new Path(output))) fileSystem.delete(new Path(output), true)
  }
}
