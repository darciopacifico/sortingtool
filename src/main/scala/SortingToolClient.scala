import java.io._
import java.util.{Comparator, PriorityQueue}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

/**
 * Created by dpacif1 on 6/14/16.
 *
 * Very large file sorting
 *
 * Hint: External sorting is a term for a class of sorting algorithms that can handle massive amounts of data.
 *
 * Problem: You have a *very* large text file, so it does not fit in memory, with text lines. Sort the file
 * into an output file where all the lines are sorted in alphabetic order, taking into account all words per
 * line. The lines themselves do not need to be sorted and are not to be modified. Lines are considered to
 * be average in length so edge cases such as a file with just two very large lines should still work but
 * it is OK if performance suffers in that case.
 *
 * Boundary: Use any programming language you feel comfortable with. Please use standard libraries only,
 * no batch or stream processing frameworks. Be as efficient as possible while avoiding using standard
 * library sorting routines. Provide a rationale for your approach. Design schemas are welcome.
 *
 * Please note that the file.txt that we use to measure the performance of the result is
 * generated via: ruby -e 'a=STDIN.readlines;5000000.times do;b=[];16.times do; b << a[rand(a.size)].chomp end; puts b.join(" "); end' < /usr/share/dict/words > file.txt
 *
 */
object SortingToolClient {

  val freeMemoryUsage = 0.5
  val bytesPerCharArray = 24 + 8 + 8
  val charSize = 2

  val bufferReaderSize = 10*1024
  val bufferWriterSize = 10*1024
  
  /**
   *
   * @param args
   */
  def main(args: Array[String]) {
    //val fileName = "/Users/dpacif1/scala/sortingtool/src/main/resources/file.txt"

    val fileName = System.getProperties.getProperty("file")
    sortFileLines(fileName)
  }


  /**
   * Sort the file name
   * @param fileName
   */
  def sortFileLines(fileName: String): Unit = {
    if (!isValid(fileName)) {
      System.out.println(s"Invalid file '$fileName'")
      printUsage()
      return
    }
    //determine the amount of available memory for process
    val totalMemInChars = (getAvailableMemory / charSize).toInt
    
    
    //create a buffer reader for original file
    val fileIterator = scala.io.Source.fromFile(new File(fileName),bufferReaderSize).getLines()

    //partitionate file
    val filePartitions = partitionateFile(fileIterator, totalMemInChars)

    //merge partitions
    mergePartitions(fileName, totalMemInChars, filePartitions)

    //delete partition files
    filePartitions.foreach(_.delete())
  }

  /**
   * Partitioning the original file
   * @param destFileName
   * @param totalMemForMergePartitions
   * @param filePartitions
   */
  def mergePartitions(destFileName: String, totalMemForMergePartitions: Int, filePartitions: List[File]): Unit = {

    System.out.println(s"Merging ${filePartitions.size} partition files!")
    //buffer space per partition file reader
    val chunkBufferSize: Int = (totalMemForMergePartitions / (filePartitions.size+1)).toInt

    //create buffer readers
    val readers: List[BufferedReader] = filePartitions.map { file =>
      new BufferedReader(new FileReader(file), chunkBufferSize)
    }

    //writer to dest file
    val fi = new File(destFileName + "_sorted_ext")
    val bw = new BufferedWriter(new FileWriter(fi), chunkBufferSize.toInt)
    val pw: PrintWriter = new PrintWriter(bw)

    //process transfer to dest with ordered reading
    mergePartitions(readers, pw)

    pw.flush()
  }

  /**
   * get free memory
   * @return
   */
  def getAvailableMemory = (Runtime.getRuntime.freeMemory() * freeMemoryUsage).toLong

  /**
   * Merge all file partition into the destination file, using a PriorityQueue to determine
   * the next line from all partitions, as follow:
   *
   * Create a PriorityQueue, initially filled with tuples of the first line of each file and the reader.
   * 
   * At each remove operation from priorityQueue, the line is written into destination file, one more line is read from bufferedReader and the combination
   * of this new line and the bufferedReader itself is added to priority queue again. All the operation is called
   * recursively until PriorityQueue becomes empty
   * 
   * @param readers
   * @param destFile
   */
  def mergePartitions(readers: List[BufferedReader], destFile: PrintWriter): Unit = {

    //define the priority queue limited to readers size
    val pq = new PriorityQueue[(String, BufferedReader)](readers.size, new Comparator[(String, BufferedReader)] {
      override def compare(o1: (String, BufferedReader), o2: (String, BufferedReader)): Int = {
        val ((lineA, _), (lineB, _)) = (o1, o2)
        lineA.compareTo(lineB)
      }
    })

    //fill the priority queue
    readers.foreach { reader =>
      val line = reader.readLine()
      if (line != null) {
        pq.add((line, reader))
      }
    }

    /**
     * Recursive method that consumes the priority queue
     */
    @tailrec
    def consumePQ(pq: PriorityQueue[(String, BufferedReader)], destFile: PrintWriter): Unit = {
      if (!pq.isEmpty) {
        // consumes until PQ becomes empty
        val (line, reader) = pq.remove()

        //append this line to the dest file
        destFile.println(line)

        //update the PQ for next recursive call
        val newLine = reader.readLine()
        if (newLine != null) {
          pq.add((newLine, reader))
        }

        //recursive call
        consumePQ(pq, destFile)
      }
    }

    //call tailrec consumer
    consumePQ(pq, destFile)
  }


  /**
   * Partitionate the original file
   * @param fileIterator
   * @param bufferInChars
   * @return
   */
  def partitionateFile(fileIterator: Iterator[String], bufferInChars:Int): List[File] = {
    val partitionFiles = new ListBuffer[File]()

    var bytesRead = 0

    val lineBuffer = new ListBuffer[String]()
    for (line <- fileIterator) {
      lineBuffer += line

      bytesRead += (line.length + bytesPerCharArray)

      if (bytesRead >= bufferInChars) {
       System.out.println(s"Writing ${lineBuffer.size} lines to partition file!")
        writeToPartFile(lineBuffer, partitionFiles)
        bytesRead = 0
      }
    }

    writeToPartFile(lineBuffer, partitionFiles)

    partitionFiles.toList
  }

  /**
   * Sort registries in memory and write to partition file
   * @param lineBuffer
   * @param filesBuffered
   */
  def writeToPartFile(lineBuffer: ListBuffer[String], filesBuffered: ListBuffer[File]): Unit = {
    val (file, fileWriter) = getFileWritter() // filewriter is 50KB bufferized

    lineBuffer.sorted.foreach(fileWriter.println) // inline sort and write to partition file
    lineBuffer.clear() // cleanup buffer

    fileWriter.flush() //

    filesBuffered += file // incremente partition file refs
  }

  /**
   * Create temp file for partition
   * @return
   */
  def getFileWritter(): (File, PrintWriter) = {
    val file = File.createTempFile("tmp_", "_fileSorting")

    file.deleteOnExit()

    val fileWriter = new FileWriter(file)

    val pw = new PrintWriter(new BufferedWriter(fileWriter, bufferWriterSize))

    (file, pw)
  }


  /**
   * Check file availability and read permission
   * @param fileName
   * @return
   */
  def isValid(fileName: String): Boolean = {

    if (fileName == null) {
      return false
    }

    val file = new File(fileName)

    file.exists && file.canRead
  }

  /**
   * Print usage of tool
   */
  def printUsage() = {
    System.out.println(
      """
        |***************************************************
        |******        VERY LARGE FILE SORTING        ******
        |***************************************************
        |
        |To sort a file execute:
        |
        |java -jar sortingTool.jar -Dfile=<file path>
        |
        |Exiting...
      """.stripMargin)
  }

}
