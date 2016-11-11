package com.godatadriven

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

/**
  * Created by Fokko Driesprong on 12/11/2016.
  */
class SourceGamesFromCsv(file: String) extends SourceFunction[Array[String]]() {

  var isRunning: Boolean = true

  override def run(ctx: SourceContext[Array[String]]) = {

    val lines = scala.io.Source.fromFile(file).getLines
    var currentReplayId = 0
    var timestamp = 0L

    var singleGame = List[String]()

    while (lines.nonEmpty) {
      val line = lines.next()
      val parts = line.split(',')

      if (parts(0) != "ReplayID") {
        val nextReplayId = Integer.parseInt(parts(0))
        if (currentReplayId != nextReplayId) {

          if (singleGame.nonEmpty)
            ctx.collect(singleGame.toArray)

          singleGame = List[String]()

          // Set everything for the next game
          currentReplayId = nextReplayId
          timestamp = System.currentTimeMillis
        }

        singleGame = (line + "," + timestamp) :: singleGame
      }
    }
  }

  override def cancel(): Unit = isRunning = false
}