package main

import java.io.{DataInput, DataOutput, File, FileWriter}
import java.lang.Iterable
import java.util

import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{MultipleInputs, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import scala.collection.JavaConversions._

import java.nio.file.{Files, Paths}

import org.apache.log4j.BasicConfigurator

// Author: Nat Tuck & Zhikai Ding

class MyKey(var playerID: String, var teamID:String, var year: Int) extends WritableComparable[MyKey] {
    def this() = this("", "", 0)


    override def write(out: DataOutput): Unit = {
        out writeUTF playerID
        out writeUTF teamID
        out writeInt year
    }

    override def readFields(in: DataInput): Unit = {
        playerID = in readUTF()
        teamID = in readUTF()
        year = in readInt()
    }

    override def compareTo(o: MyKey): Int = {
        if (year != o.year) {
            year.compare(o.year)
        } else if (!teamID.equals(o.teamID)) {
            teamID.compareTo(o.teamID)
        } else {
            playerID.compareTo(o.playerID)
        }
    }


    override def equals(obj: scala.Any): Boolean = obj match {
        case obj:MyKey => obj.playerID.equals(playerID) && obj.year == year && teamID.equals(obj.teamID)
        case _ => false
    }

    override def hashCode(): Int = {
        var hashcode: Int = 0
        hashcode = hashcode * 31 + year.hashCode
        hashcode = hashcode * 31 + playerID.hashCode
        hashcode = hashcode * 31 + teamID.hashCode
        hashcode
    }
}


class MyValue(var valueType: Int, var value: Int) extends Writable {

    def this() = this(0, 0)

    override def write(out: DataOutput): Unit = {
        out writeInt valueType
        out writeInt value
    }

    override def readFields(in: DataInput): Unit = {
        valueType = in readInt()
        value = in readInt()
    }
}


object Demo {


    def main(args: Array[String]) {
        println("Demo: startup")

        // Configure log4j to actually log.
        BasicConfigurator.configure();

        // Make a job
        val job = Job.getInstance()
        job.setJarByClass(Demo.getClass)
        job.setJobName("Demo")

        // Set classes mapper, reducer, input, output.
        job.setReducerClass(classOf[BaseballReducer])

        job.setMapOutputKeyClass(classOf[MyKey])
        job.setMapOutputValueClass(classOf[MyValue])
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[Text])

        // Set up number of mappers, reducers.
        job.setNumReduceTasks(1)

        MultipleInputs.addInputPath(job, new Path("baseball/Salaries.csv"),
            classOf[TextInputFormat], classOf[SalaryMapper])
        MultipleInputs.addInputPath(job, new Path("baseball/Batting.csv"),
            classOf[TextInputFormat], classOf[HomerunMapper])

        if (Files.exists(Paths.get("out"))) {
            FileUtil.fullyDelete(new File("out"))
        }

        if (Files.exists(Paths.get("min_values.txt"))) {
            Files.delete(Paths.get("min_values.txt"))
        }

        FileOutputFormat.setOutputPath(job, new Path("out"))

        // Actually run the thing.
        job.waitForCompletion(true)
    }
}


class MinimalFinderMapper extends Mapper[Text, Text, Text, Text] {
    type Context = Mapper[Text, Text, Text, Text]#Context

    def parseDouble(s: String) = try { Some(s.toDouble) } catch { case _ => None }

    override def map(key: Text, value: Text, ctx: Context): Unit = {
        val keyCols = key.toString.split(" ")
        val payedPerHR:Double = value.toString.toDouble
    }
}


// Author: Nat Tuck
class SalaryMapper extends Mapper[Object, Text, MyKey, MyValue] {
    type Context = Mapper[Object, Text, MyKey, MyValue]#Context

    override def map(_k: Object, vv: Text, ctx: Context) {
        println(vv)
        val cols = vv.toString.split(",")
        if (cols(0) == "yearID") {
            return
        }

        try {
            val salary = cols(4).toInt
            val key = new MyKey(cols(3), cols(1), cols(0).toInt)
            val value = new MyValue(0, salary)

            if (salary > 0) {
                ctx.write(key, value)
            } else {
                println("empty")
            }
        } catch {
            case _: Throwable => print("fk u")
        }
    }
}

class HomerunMapper extends Mapper[Object, Text, MyKey, MyValue] {
    type Context = Mapper[Object, Text, MyKey, MyValue]#Context

    var players: util.HashMap[MyKey, Int] = new util.HashMap[MyKey, Int]()

    override def map(_k: Object, vv: Text, ctx: Context) {
        println(vv)
        val cols = vv.toString.split(",")
        if (cols(0) == "playerID") {
            return
        }

        try {
            val hr = cols(11).toInt
            val key = new MyKey(cols(0), cols(3), cols(1).toInt)
            if (!players.containsKey(key)) {
                players.put(key, 0)
            }
            players.put(key, players.get(key) + hr)
        } catch {
            case _: Throwable => print("fk u")
        }
    }

    override def cleanup(context: Context): Unit = {
        for (k <- players.keySet()) {
            if (players.get(k) > 0) {
                context.write(k, new MyValue(1, players.get(k)))
            }
        }
    }
}

class BaseballReducer extends Reducer[MyKey, MyValue, Text, Text] {
    type Context = Reducer[MyKey, MyValue, Text, Text]#Context
    var minValue: Double = Double.MaxValue
    var minKey: MyKey = null
    var hr = 0
    var salary = 0

    override def reduce(key: MyKey, values: Iterable[MyValue], context: Context): Unit = {
        var payed = 0
        var homeRun = 0

        var a = 0
        var b = 0

        val k: util.ArrayList[MyValue] = new util.ArrayList[MyValue]()

        for (value: MyValue <- values) {
            k.add(new MyValue(value.valueType, value.value))
            if (value.valueType == 0) {
                payed = value.value
                a += 1
            } else {
                homeRun = value.value
                b += 1
            }
        }
        if (a + b > 2) {
            print("weird!!!\n")
        }
        if (a == 1 && b == 1) {
            if (homeRun != 0) {
                val perHR = payed.toFloat / homeRun
                if (perHR < minValue && perHR > 0) {
                    minValue = perHR
                    salary = payed
                    hr = homeRun
                    minKey = new MyKey(key.playerID, key.teamID, key.year)
                }
                context.write(new Text(key.playerID + " " + key.year), new Text(perHR.toString))
            }
        }
    }

    override def cleanup(context: Context): Unit = {
        val fw = new FileWriter("min_values.txt", true)
        try {
            fw.write(minKey.playerID + " " + minKey.year +
                " " + minKey.teamID + " " + minValue.toString +
                " " + hr + " " + salary + "\n")
        }
        finally fw.close()
    }
}

// vim: set ts=4 sw=4 et:
