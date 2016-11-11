package com.godatadriven

/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */


import java.util.concurrent.TimeUnit._

import scala.util.Random
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow, Window}
import org.apache.flink.streaming.api.functions.sink.SocketClientSink
import org.apache.flink.streaming.util.serialization.{SerializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.util.Collector
import java.beans.Transient
import java.util.concurrent.TimeUnit

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object Job {

  val heroes = Map(
    (0, ("Unknown", "", "")),
    (1, ("Abathur", "Specialist", "Utility")),
    (2, ("Anub arak", "Warrior", "Bruiser")),
    (3, ("Arthas", "Warrior", "Bruiser")),
    (4, ("Azmodan", "Specialist", "Siege")),
    (5, ("Brightwing", "Support", "Healer")),
    (6, ("Chen", "Warrior", "Tank")),
    (7, ("Diablo", "Warrior", "Tank")),
    (8, ("E.T.C.", "Warrior", "Tank")),
    (9, ("Falstad", "Assassin", "Sustained Damage")),
    (10, ("Gazlowe", "Specialist", "Siege")),
    (11, ("Illidan", "Assassin", "Sustained Damage")),
    (12, ("Jaina", "Assassin", "Burst Damage")),
    (13, ("Johanna", "Warrior", "Tank")),
    (14, ("Kael'thas", "Assassin", "Burst Damage")),
    (15, ("Kerrigan", "Assassin", "Ambusher")),
    (16, ("Kharazim", "Support", "Healer")),
    (17, ("Leoric", "Warrior", "Bruiser")),
    (18, ("Li Li", "Support", "Healer")),
    (19, ("Malfurion", "Support", "Healer")),
    (20, ("Muradin", "Warrior", "Tank")),
    (21, ("Murky", "Specialist", "Utility")),
    (22, ("Nazeebo", "Specialist", "Sustained Damage")),
    (23, ("Nova", "Assassin", "Ambusher")),
    (24, ("Raynor", "Assassin", "Sustained Damage")),
    (25, ("Rehgar", "Support", "Healer")),
    (26, ("Sgt. Hammer", "Specialist", "Siege")),
    (27, ("Sonya", "Warrior", "Bruiser")),
    (28, ("Stitches", "Warrior", "Tank")),
    (29, ("Sylvanas", "Specialist", "Siege")),
    (30, ("Tassadar", "Support", "Support")),
    (31, ("The Butcher", "Assassin", "Ambusher")),
    (32, ("The Lost Vikings", "Specialist", "Utility")),
    (33, ("Thrall", "Assassin", "Sustained Damage")),
    (34, ("Tychus", "Assassin", "Sustained Damage")),
    (35, ("Tyrael", "Warrior", "Bruiser")),
    (36, ("Tyrande", "Support", "Support")),
    (37, ("Uther", "Support", "Healer")),
    (38, ("Valla", "Assassin", "Sustained Damage")),
    (39, ("Zagara", "Specialist", "Siege")),
    (40, ("Zeratul", "Assassin", "Ambusher")),
    (41, ("Rexxar", "Warrior", "Tank")),
    (42, ("Lt. Morales", "Support", "Healer")),
    (43, ("Artanis", "Warrior", "Bruiser")),
    (44, ("Cho", "Warrior", "Tank")),
    (45, ("Gall", "Assassin", "Sustained Damage")),
    (46, ("Lunara", "Assassin", "Sustained Damage")),
    (47, ("Greymane", "Assassin", "Sustained Damage"))
  )


  def main(args: Array[String]) {
    case class HeroReplay(replayId: Int, autoSelect: Boolean, heroId: Int, heroLevel: Int, isWinner: Boolean, mmr: Int, eventTime: Long)
    case class HeroResults[A](key: A, wins: Int, total: Int = 1)
    case class HeroDescription(heroId: Int, name: String, group: String)

    // This will convert a boolean to an int
    implicit def bool2int(b: Boolean): Int = if (b) 1 else 0

    def parseCsvString(parts: Array[String]): HeroReplay = {
      HeroReplay(
        parts(0).toInt,
        parts(1).toBoolean,
        parts(2).toInt,
        parts(3).toInt,
        parts(4).toBoolean,
        parts(5).toInt,
        parts(6).toLong
      )
    }
    // The source from where we can build different processing pipelines
    val gamesSource = senv.addSource(new SourceGamesFromCsv("ReplayCharacters.csv"))

    gamesSource

      .writeAsCsv("/tmp/hots_output.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1)

    senv.execute("HOTS")
  }
}
